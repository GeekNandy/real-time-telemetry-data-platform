#!/usr/bin/env python3
"""Summarize observed capacity budgets and JFR samples without extrapolating production capacity."""
import argparse
from collections import Counter
from datetime import datetime, timedelta
import json
import math
import hashlib
from pathlib import Path
import re
import subprocess
import tempfile


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def seconds(value):
    if isinstance(value, (float, int)):
        return float(value)
    # JFR JSON represents durations as ISO-8601 values, including subsecond events.
    match = re.fullmatch(r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", value)
    if not match or not any(match.groups()):
        raise ValueError("Unsupported JFR duration")
    return sum(float(part or 0) * scale for part, scale in zip(match.groups(), (3600, 60, 1)))


def quantiles(values):
    values = sorted(values)
    return {"count": len(values), **{
        f"p{p}": values[math.ceil(p / 100 * len(values)) - 1] if values else None
        for p in (50, 95, 99)}, "max": values[-1] if values else None}


def frame_name(frame):
    method = frame.get("method", {})
    return method.get("type", {}).get("name", "unknown").replace("/", ".") + "." + method.get("name", "unknown")


def jfr_events(events, start, end):
    counts, stacks, allocations = Counter(), Counter(), Counter()
    pauses, parks, files = [], [], []
    allocated_weight = 0
    cpu = []
    cpu_coverage = []
    for event in events:
        kind = event["type"]
        values = event["values"]
        observed = timestamp(values["startTime"])
        if kind == "jdk.CPULoad":
            cpu_coverage.append(observed)
        if not start <= observed <= end:
            continue
        counts[kind] += 1
        frames = (values.get("stackTrace") or {}).get("frames", [])
        if kind == "jdk.ExecutionSample" and frames:
            stacks[frame_name(frames[0])] += 1
        if kind == "jdk.ObjectAllocationSample":
            weight = int(values.get("weight", 0))
            if weight < 0:
                raise ValueError("Negative allocation sample weight")
            allocated_weight += weight
            allocations[(values.get("objectClass") or {}).get("name", "unknown").replace("/", ".")] += weight
        if kind == "jdk.GarbageCollection":
            pauses.append(seconds(values.get("sumOfPauses", values.get("duration", "PT0S"))) * 1000)
        if kind == "jdk.ThreadPark":
            parks.append(seconds(values["duration"]) * 1000)
        if kind in ("jdk.FileRead", "jdk.FileWrite"):
            files.append(seconds(values["duration"]) * 1000)
        if kind == "jdk.CPULoad":
            cpu.append(float(values.get("jvmUser", 0)) + float(values.get("jvmSystem", 0)))
    coverage = bool(cpu_coverage) and min(cpu_coverage) <= start + timedelta(seconds=3) and max(cpu_coverage) >= end - timedelta(seconds=3)
    return {
        "window_start": start.isoformat(), "window_end": end.isoformat(), "cpu_sample_coverage": coverage,
        "event_counts": dict(counts), "sampled_top_methods": stacks.most_common(15),
        "estimated_allocation_bytes": allocated_weight,
        "sampled_allocation_classes_by_weight": allocations.most_common(15),
        "gc_pause_ms": quantiles(pauses), "gc_pause_total_ms": sum(pauses),
        "thresholded_thread_park_ms": quantiles(parks), "thresholded_file_io_ms": quantiles(files),
        "jvm_cpu_fraction": quantiles(cpu),
        "interpretation": "JFR sampling and configured event thresholds apply. Allocation weights are estimates. File/park events exclude operations below recording thresholds; sampled stack counts are not exact CPU percentages."
    }


def jfr_report(recording, load, output):
    start = timestamp(load["started_at_utc"])
    end = start + timedelta(seconds=load["generator"]["scheduled_duration_s"])
    selected = "jdk.ExecutionSample,jdk.ObjectAllocationSample,jdk.JavaMonitorEnter,jdk.ThreadPark,jdk.FileRead,jdk.FileWrite,jdk.GarbageCollection,jdk.CPULoad"
    with tempfile.TemporaryFile(mode="w+") as raw:
        subprocess.run(["jfr", "print", "--json", "--stack-depth", "32", "--events", selected, str(recording)], stdout=raw, check=True)
        raw.seek(0)
        result = jfr_events(json.load(raw)["recording"]["events"], start, end)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def workload_report(load, lag, operations):
    start = timestamp(load["started_at_utc"])
    duration = load["generator"]["scheduled_duration_s"]
    end = start + timedelta(seconds=duration)
    lag = [p for p in lag if start <= timestamp(p["observed_at"]) <= end]
    operations = [p for p in operations if start <= timestamp(p["observed_at"]) <= end]
    good_lag = [p["lag_records"] for p in lag if p.get("available")]
    buffers = [p["collector"]["bufferRecords"] for p in operations if isinstance(p.get("collector"), dict)
               and p["collector"].get("bufferRecords") is not None]
    scheduled = load["scheduled"]
    accepted = load["outcomes"].get("202", 0)
    if scheduled <= 0 or accepted > scheduled:
        raise ValueError("Invalid offered/accepted counts")
    rejection_fraction = 1 - accepted / scheduled
    rate = load["generator"]["rate_per_s"]
    coverage = len(good_lag) >= duration * 0.9 and len(buffers) >= duration / 5 * 0.8
    budget = {
        "all_accepted_ids_visible": load["accepted_reconciliation"]["passed"],
        "offered_to_202_fraction_at_least_99_percent": rejection_fraction <= 0.01,
        "p99_scheduled_to_response_at_most_1000ms": load["successful_scheduled_to_response"]["p99_ms"] is not None
            and load["successful_scheduled_to_response"]["p99_ms"] <= 1000,
        "generator_p99_delay_at_most_50ms": load["generator_schedule_delay"]["p99_ms"] is not None
            and load["generator_schedule_delay"]["p99_ms"] <= 50,
        "source_lag_at_most_two_seconds_of_offered_traffic": bool(good_lag) and max(good_lag) <= rate * 2,
        "observer_coverage": coverage,
    }
    return {
        "load": load, "budget": budget, "budget_passed": all(budget.values()),
        "rejection_or_client_failure_fraction": rejection_fraction,
        "source_lag_records": quantiles(good_lag), "buffer_records": quantiles(buffers),
        "source_lag_observations": len(lag), "source_lag_available": len(good_lag),
        "interpretation": "Observed budgets for this workload and shared runner. Post-load query visibility is an upper bound, not a database-commit latency SLO. 503 and transport outcomes can be delivered later."
    }


def read_jsonl(path):
    records = []
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and "observed_at" in value:
            records.append(value)
    return records


def aggregate(directory):
    expected = {(rate, repeat, phase) for rate in (10, 50, 150) for repeat in (1, 2, 3)
                for phase in ("spool-baseline", "acknowledged", "acknowledged-profiled")}
    observations = {}
    environments = []
    for path in sorted(directory.rglob("environment.json")):
        value = json.loads(path.read_text())
        if value["mode"] != "full" or value["dirty"]:
            raise ValueError("Pilot or dirty-tree evidence cannot establish a full workload result")
        cpu = json.loads(value["cpu"])["lscpu"]
        environments.append({"revision": value["revision"], "tree": value["tree"],
            "runner": json.loads(value["runner"]),
            "cpu_model": next((r["data"] for r in cpu if r["field"] == "Model name:"), "unknown"),
            "java": value["java"].splitlines()[0], "workflow_url": value.get("workflow_url"),
            "started_at": value["started_at"]})
    for path in sorted(directory.rglob("report.json")):
        value = json.loads(path.read_text())
        key = value["rate"], value["repeat"], value["phase"]
        if key not in expected or key in observations:
            raise ValueError("Unexpected or duplicated rate/repetition/phase")
        load = value["load"]
        if not value["capacity_eligible"] or load["generator"]["scheduled_duration_s"] != 600 or load["working_tree_dirty"]:
            raise ValueError("Require clean ten-minute workload evidence")
        observation = {"rate": key[0], "repeat": key[1], "phase": key[2],
            "runtime_revision": value["source_revision"], "generator_revision": load["revision"],
            "duration_seconds": 600, "offered": load["scheduled"], "outcomes": load["outcomes"],
            "acknowledged_per_s_including_drain": load["acknowledged_per_s_including_inflight_drain"],
            "successful_http_elapsed": load["successful_http_elapsed"],
            "successful_scheduled_to_response": load["successful_scheduled_to_response"],
            "generator_schedule_delay": load["generator_schedule_delay"],
            "accepted_reconciliation": load["accepted_reconciliation"],
            "ambiguous_client_outcomes": load["ambiguous_client_outcomes"],
            "source_lag_records": value["source_lag_records"], "buffer_records": value["buffer_records"],
            "resources": value["resources"], "budget": value["budget"], "budget_passed": value["budget_passed"],
            "workload": value["workload"], "report_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        if key[2] == "acknowledged-profiled":
            observation["jfr"] = {role: {**value["jfr"][role],
                "analysis": json.loads(path.with_name(role + "-jfr.json").read_text())} for role in ("collector", "worker")}
        observations[key] = observation
    if set(observations) != expected or len(environments) != 9:
        raise ValueError("Require all three rates, three fresh repetitions and all three phases")
    revisions = {e["revision"] for e in environments}
    if len(revisions) != 1 or any(row["generator_revision"] not in revisions for row in observations.values()):
        raise ValueError("Evidence mixes generator revisions")
    passing = [rate for rate in (10, 50, 150)
               if all(observations[(rate, repeat, "acknowledged")]["budget_passed"] for repeat in (1, 2, 3))]
    comparisons = []
    for rate in (10, 50, 150):
        for repeat in (1, 2, 3):
            baseline, acknowledged, profiled = [observations[(rate, repeat, phase)] for phase in
                ("spool-baseline", "acknowledged", "acknowledged-profiled")]
            def percent(a, b):
                return 100 * (a / b - 1) if a is not None and b is not None and b > 0 else None
            comparisons.append({"rate": rate, "repeat": repeat,
                "acknowledgement_p99_delta_ms": acknowledged["successful_http_elapsed"]["p99_ms"] - baseline["successful_http_elapsed"]["p99_ms"]
                    if acknowledged["successful_http_elapsed"]["p99_ms"] is not None and baseline["successful_http_elapsed"]["p99_ms"] is not None else None,
                "profiled_p99_change_percent": percent(profiled["successful_http_elapsed"]["p99_ms"], acknowledged["successful_http_elapsed"]["p99_ms"]),
                "profiled_acknowledged_rate_change_percent": percent(profiled["acknowledged_per_s_including_drain"], acknowledged["acknowledged_per_s_including_drain"])})
    return {"schema_version": 1, "environments": environments, "observations": [observations[k] for k in sorted(observations)],
        "comparisons": comparisons, "highest_passing_tested_offered_rate": max(passing, default=None),
        "interpretation": "Observed shared-runner workload budgets across three fresh repetitions. This identifies tested passing points, not a production capacity ceiling. Percentile differences compare run-level percentiles and do not pool latency samples. Generator/observer contention, unknown storage IOPS and host variation remain limitations."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregate", type=Path)
    parser.add_argument("recording", type=Path, nargs="?")
    parser.add_argument("load_summary", type=Path, nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    args = parser.parse_args()
    if args.aggregate:
        print(json.dumps(aggregate(args.aggregate), indent=2, sort_keys=True))
    elif all((args.recording, args.load_summary, args.output)):
        jfr_report(args.recording, json.loads(args.load_summary.read_text()), args.output)
    else:
        parser.error("Provide --aggregate DIRECTORY or RECORDING LOAD_SUMMARY OUTPUT")
