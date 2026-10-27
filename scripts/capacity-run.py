#!/usr/bin/env python3
"""Run fresh, paired three-broker experiments on an ephemeral Linux Docker host."""
import argparse
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASELINE = "7df5bb9d56bb666b95c00b36bfb13af12cb02130"
spec = importlib.util.spec_from_file_location("capacity_report", ROOT / "scripts/capacity-report.py")
REPORT = importlib.util.module_from_spec(spec)
spec.loader.exec_module(REPORT)


def run(command, *, output=None, timeout=900, check=True):
    if output:
        with output.open("w") as log:
            return subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                  timeout=timeout, check=check)
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=check)


def compress(source, target):
    with source.open("rb") as input_file, gzip.open(target, "wb", compresslevel=9) as output_file:
        shutil.copyfileobj(input_file, output_file)


def now():
    return datetime.now(timezone.utc).isoformat()


def operations(token, url):
    request = urllib.request.Request(url + "/v1/operations/metrics", headers={"Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            data = response.read(65537)
            return json.loads(data) if len(data) <= 65536 else None
    except urllib.error.HTTPError as error:
        error.close()
        return None
    except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError):
        return None


def sample(compose, token, output, stopped):
    with output.open("w") as target:
        while not stopped.is_set():
            started = time.monotonic()
            value = {"observed_at": now(), "collector": operations(token, "http://127.0.0.1:8080"),
                     "worker": operations(token, "http://127.0.0.1:8082")}
            try:
                ids = run(compose + ["ps", "-q"], timeout=10).stdout.split()
                if not ids:
                    raise ValueError("No experiment containers")
                stats = run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids], timeout=10)
                value["containers"] = [json.loads(line) for line in stats.stdout.splitlines()]
            except (subprocess.SubprocessError, ValueError):
                value["containers"] = None
            target.write(json.dumps(value, sort_keys=True) + "\n")
            target.flush()
            stopped.wait(max(0, 5 - (time.monotonic() - started)))


def container_states(compose):
    ids = run(compose + ["ps", "-q", "--all"], timeout=15).stdout.split()
    template = ('{"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
                '"image_id":{{json .Image}},"memory_limit_bytes":{{.HostConfig.Memory}},'
                '"nano_cpus":{{.HostConfig.NanoCpus}},"oom_killed":{{.State.OOMKilled}},'
                '"restart_count":{{.RestartCount}},"running":{{.State.Running}}}')
    return [json.loads(run(["docker", "inspect", "--format", template, identifier], timeout=10).stdout)
            for identifier in ids]


def load(token_file, output, rate, duration):
    result = run(["python3", "scripts/load.py", "--token-file", str(token_file), "--rate", str(rate),
                  "--duration", str(duration), "--concurrency", "64", "--settle", "600", "--timeout", "10",
                  "--output", str(output)], output=output.parent / (output.name + ".log"),
                 timeout=duration + 650, check=False)
    summary = json.loads((output / "summary.json").read_text())
    if summary["accepted_reconciliation"]["unresolved_ids"]:
        raise RuntimeError("Accepted-ID reconciliation failed; retain the experiment reports")
    if result.returncode and summary["accepted_reconciliation"]["accepted"]:
        raise RuntimeError("Load client failed after accepting events")
    return summary


def start_probe(compose, directory):
    jar_path = run(compose + ["exec", "-T", "collector", "sh", "-c", "ls /app/lib/kafka-clients-*.jar"], timeout=15).stdout.strip()
    if not jar_path.startswith("/app/lib/kafka-clients-") or "\n" in jar_path or not jar_path.endswith(".jar"):
        raise RuntimeError("Cannot identify the packaged Kafka client")
    run(compose + ["cp", "collector:" + jar_path, str(directory / "kafka-clients.jar")], timeout=30)
    run(["javac", "--release", "17", "-Xlint:all", "-Werror", "-cp", str(directory / "kafka-clients.jar"),
         "-d", str(directory), "tools/capacity-probe/src/CapacityProbe.java"], timeout=30)
    run(compose + ["cp", str(directory / "CapacityProbe.class"), "kafka:/tmp/CapacityProbe.class"], timeout=30)
    log = (directory / "source-lag.raw.log").open("w")
    process = subprocess.Popen(compose + ["exec", "-T", "-e", "JAVA_TOOL_OPTIONS=", "kafka", "java", "-Xms16m", "-Xmx96m",
        "-cp", "/opt/kafka/libs/*:/tmp", "CapacityProbe", "kafka:9092,kafka2:9092,kafka3:9092",
        "telemetry.events.v1", "telemetry-persistence-v1", "1800"], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log.close()
            raise RuntimeError("Source observer failed: " + (directory / "source-lag.raw.log").read_text()[-4000:])
        if REPORT.read_jsonl(directory / "source-lag.raw.log"):
            return process, log
        time.sleep(0.2)
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    log.close()
    raise RuntimeError("Source observer produced no bounded observations")


def resource_report(samples, start, end):
    cpu, memory = {}, {}
    for sample in samples:
        if not start <= REPORT.timestamp(sample["observed_at"]) <= end:
            continue
        for container in sample.get("containers") or []:
            # Names are generated locally; no environment or application payload is inspected.
            role = container["Name"].rsplit("-", 2)[-2]
            cpu.setdefault(role, []).append(float(container["CPUPerc"].rstrip("%")))
            memory.setdefault(role, []).append(float(container["MemPerc"].rstrip("%")))
    return {role: {"cpu_percent": REPORT.quantiles(cpu[role]), "memory_percent_of_limit": REPORT.quantiles(memory[role])}
            for role in cpu}


def phase(args, name, env_file, private):
    directory = args.output / name
    directory.mkdir()
    artifact = args.output / "artifact" / name
    artifact.mkdir(parents=True)
    image = "telemetry-capacity:baseline" if name == "spool-baseline" else "telemetry-capacity:current"
    override = {"services": {}}
    for role in ("collector", "worker"):
        options = "-XX:MaxRAMPercentage=65 -XX:ActiveProcessorCount=1"
        if name == "acknowledged-profiled":
            options += f" -XX:StartFlightRecording=filename=/profiles/{role}.jfr,settings=/app/profile-safe.jfc,dumponexit=true,maxsize=64m,maxage=30m"
        override["services"][role] = {"image": image, "environment": {
            "JAVA_TOOL_OPTIONS": options, "TRACING_SAMPLE_RATIO": "0.1",
            "LOGGING_LEVEL_ORG_APACHE_KAFKA": "WARN"}}
    override_path = directory / "override.json"
    override_path.write_text(json.dumps(override, indent=2) + "\n")
    shutil.copyfile(override_path, artifact / "runtime-override.json")
    project = f"capacity-{args.rate}-{args.repeat}-{name}"
    compose = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.replicated.yaml",
               "-f", str(override_path), "--env-file", str(env_file), "-p", project]
    token_file = private / "token"
    token_file.write_text(run(["python3", "scripts/demo-token.py", "--env-file", str(env_file),
                               "--tenant", "capacity", "--console"]).stdout.strip())
    token_file.chmod(0o600)
    probe = None
    probe_log = None
    monitor = None
    stopped = threading.Event()
    try:
        print(f"Starting {name}: {args.rate}/s, repetition {args.repeat}", flush=True)
        run(compose + ["up", "-d", "--no-build", "collector", "worker"], output=directory / "startup.log", timeout=420)
        run(["python3", "scripts/wait-ready.py"], output=directory / "readiness.log", timeout=240)
        states_before = container_states(compose)
        fs = run(compose + ["exec", "-T", "collector", "stat", "-f", "-c", "%T %S %b %a", "/app/data/wal"], timeout=10).stdout.strip()
        probe, probe_log = start_probe(compose, directory)
        monitor = threading.Thread(target=sample, args=(compose, token_file.read_text(), directory / "resources.jsonl", stopped), daemon=True)
        monitor.start()
        warmup = load(token_file, directory / "warmup", min(args.rate, 10), 15 if args.mode == "pilot" else 60)
        if warmup["outcomes"] != {"202": warmup["scheduled"]}:
            raise RuntimeError("Warmup did not accept and reconcile every offered event")
        print(f"Measuring {name}: {args.duration} seconds", flush=True)
        measurement = load(token_file, directory / "load", args.rate, args.duration)
        stopped.set()
        monitor.join(timeout=30)
        if monitor.is_alive():
            raise RuntimeError("Resource sampler did not stop within its budget")
        if probe.poll() is not None:
            raise RuntimeError("Source-lag observer exited during the workload")
        states_after = container_states(compose)
        resources = REPORT.read_jsonl(directory / "resources.jsonl")
        probe_log.flush()
        lag = REPORT.read_jsonl(directory / "source-lag.raw.log")
        result = REPORT.workload_report(measurement, lag, resources)
        start = REPORT.timestamp(measurement["started_at_utc"])
        end = start + timedelta(seconds=args.duration)
        result["resources"] = resource_report(resources, start, end)
        result["budget"]["no_container_oom_or_restart"] = all(not c["oom_killed"] and c["restart_count"] == 0 and c["running"] for c in states_after)
        result["budget"]["application_memory_below_90_percent_of_limit"] = all(
            role in result["resources"] and result["resources"][role]["memory_percent_of_limit"]["max"] < 90 for role in ("collector", "worker"))
        result["budget_passed"] = all(result["budget"].values())
        result.update({"phase": name, "profiled": name == "acknowledged-profiled", "rate": args.rate,
            "repeat": args.repeat, "capacity_eligible": args.mode == "full" and args.duration == 600,
            "image": image, "states_before": states_before, "states_after": states_after,
            "spool_filesystem_stat": fs, "warmup": warmup,
            "source_revision": BASELINE if name == "spool-baseline" else run(["git", "rev-parse", "HEAD"]).stdout.strip(),
            "workload": {"tenants": 1, "payload": "scripts/load.py performance.sample envelope; one synthetic attribute",
                "role_cpu_quota": 1, "role_memory_limit_bytes": 805306368, "tracing_sample_ratio": 0.1,
                "replicas": 3, "minimum_isr": 2, "producer_acks": "all", "generator_on_same_host": True,
                "storage_iops": "Unknown shared GitHub-runner local-volume IOPS"}})
        (artifact / "source-lag.jsonl").write_text("".join(json.dumps(p, sort_keys=True) + "\n" for p in lag))
        compress(directory / "resources.jsonl", artifact / "resources.jsonl.gz")
        compress(directory / "load/requests.jsonl", artifact / "requests.jsonl.gz")
        run(compose + ["stop", "-t", "30", "collector", "worker"], output=directory / "stop.log", timeout=90)
        if name == "acknowledged-profiled":
            profiles = directory / "profiles"
            run(compose + ["cp", "collector:/profiles", str(profiles)], timeout=45)
            result["jfr"] = {}
            for role in ("collector", "worker"):
                recording = profiles / (role + ".jfr")
                analysis = REPORT.jfr_report(recording, measurement, artifact / (role + "-jfr.json"))
                if not analysis["cpu_sample_coverage"] or not analysis["event_counts"].get("jdk.ExecutionSample"):
                    raise RuntimeError("JFR does not cover the measurement window")
                result["jfr"][role] = {"sha256": hashlib.sha256(recording.read_bytes()).hexdigest(), "bytes": recording.stat().st_size}
                # Keep one small representative recording pair; every case retains its full aggregate analysis.
                if args.rate == 10 and args.repeat == 1:
                    candidate = artifact / (role + ".jfr.gz")
                    compress(recording, candidate)
                    if candidate.stat().st_size > 8 * 1024 * 1024:
                        candidate.unlink()
                    else:
                        result["jfr"][role]["raw_retained"] = True
        (artifact / "report.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return result
    finally:
        stopped.set()
        if monitor:
            monitor.join(timeout=30)
        if probe:
            probe.terminate()
            try:
                probe.wait(timeout=10)
            except subprocess.TimeoutExpired:
                probe.kill()
                probe.wait(timeout=10)
        if probe_log:
            probe_log.close()
        run(compose + ["logs", "--no-color", "--tail", "80"], output=artifact / "application-tail.log", check=False, timeout=30)
        run(compose + ["down", "--volumes", "--remove-orphans"], output=directory / "cleanup.log", check=False, timeout=120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("pilot", "full"), required=True)
    parser.add_argument("--rate", type=int, choices=(10, 50, 150), required=True)
    parser.add_argument("--repeat", type=int, choices=(1, 2, 3), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.duration = 60 if args.mode == "pilot" else 600
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    artifact = args.output / "artifact"
    artifact.mkdir()
    metadata = {"started_at": now(), "mode": args.mode, "baseline_revision": BASELINE,
        "revision": run(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "workflow_url": os.environ.get("GITHUB_SERVER_URL", "https://github.com") + "/" + os.environ.get("GITHUB_REPOSITORY", "GeekNandy/real-time-telemetry-data-platform") + "/actions/runs/" + os.environ.get("GITHUB_RUN_ID", "local"),
        "tree": run(["git", "rev-parse", "HEAD^{tree}"]).stdout.strip(),
        "dirty": bool(run(["git", "status", "--porcelain"]).stdout.strip()),
        "runner": run(["docker", "info", "--format", '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"storage_driver":{{json .Driver}}}']).stdout.strip(),
        "cpu": run(["lscpu", "-J"]).stdout, "java": run(["java", "-version"]).stderr,
        "docker": run(["docker", "version", "--format", "{{json .Server.Version}}"]).stdout.strip(),
        "source_lag_observer": "One bounded Java admin observer in the first Kafka container, sampling once per second",
        "limitations": ["Generator, containers and observers share one standard GitHub runner; this is not an isolated production capacity benchmark.",
            "Host storage IOPS and noisy-neighbor effects are not controlled.", "Three data brokers share one controller; controller-quorum and AZ availability are not established.",
            "Only the stated offered rates, payload, one-tenant mix, quotas and sample ratio were tested."]}
    if metadata["dirty"]:
        raise RuntimeError("Capacity evidence requires a clean tested source tree")
    (artifact / "environment.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    baseline = args.output / "baseline-source"
    baseline.mkdir()
    archive = args.output / "baseline.tar"
    run(["git", "archive", "--format=tar", "--output", str(archive), BASELINE])
    with tarfile.open(archive) as contents:
        contents.extractall(baseline, filter="data")
    print("Building the pinned baseline and current images", flush=True)
    run(["docker", "build", "-t", "telemetry-capacity:baseline", str(baseline)], output=args.output / "baseline-build.log", timeout=900)
    run(["docker", "build", "-t", "telemetry-capacity:current", str(ROOT)], output=args.output / "current-build.log", timeout=900)
    phases = ["spool-baseline", "acknowledged", "acknowledged-profiled"]
    if args.repeat % 2 == 0:
        phases.reverse()
    results = []
    with tempfile.TemporaryDirectory(prefix="telemetry-capacity-") as temporary:
        private = Path(temporary)
        env_file = private / "env"
        env_file.write_text("DEMO_JWT_SECRET=" + secrets.token_hex(32) + "\nDATABASE_PASSWORD=" + secrets.token_hex(24) + "\n")
        env_file.chmod(0o600)
        for name in phases:
            results.append(phase(args, name, env_file, private))
    (artifact / "case.json").write_text(json.dumps({"rate": args.rate, "repeat": args.repeat, "mode": args.mode,
        "phase_order": phases, "results": [{"phase": r["phase"], "budget_passed": r["budget_passed"]} for r in results]}, indent=2) + "\n")
    print("Experiment complete; failing workload budgets are observations, not discarded runs.", flush=True)


if __name__ == "__main__":
    main()
