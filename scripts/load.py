#!/usr/bin/env python3
"""Open-loop HTTP experiment with explicit client drops and accepted-ID reconciliation.

All generated events are synthetic. Output contains IDs and measurements, never tokens.
A polling observation is an upper bound on visibility latency, not a database commit timestamp.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def call(url, token, timeout, body=None):
    request = urllib.request.Request(url, data=body, headers={
        "Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(65536)
            return response.status
    except urllib.error.HTTPError as error:
        error.read(65536)
        return error.code
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return "transport_error"


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    return {"count": len(ordered), **{
        f"p{percent}_ms": round(ordered[math.ceil(percent / 100 * len(ordered)) - 1] * 1000, 3)
        for percent in (50, 95, 99)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-url", default="http://127.0.0.1:8080")
    parser.add_argument("--worker-url", default="http://127.0.0.1:8082")
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--settle", type=float, default=60)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (0 < args.rate <= 100000 and 0 < args.duration <= 3600
            and args.rate * args.duration <= 100000 and 1 <= args.concurrency <= 128
            and 0 < args.timeout <= 60 and 0 < args.settle <= 600):
        parser.error("Require bounded positive settings and at most 100000 scheduled requests")
    for url in (args.collector_url, args.worker_url):
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
            parser.error("Use HTTP(S) endpoints without embedded credentials")
    token = args.token_file.read_text().strip()
    if not token or len(token) > 16384:
        parser.error("Token file is empty or oversized")
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    lock = threading.Lock()
    slots = threading.BoundedSemaphore(args.concurrency)
    start_wall = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()
    planned = math.ceil(args.rate * args.duration)

    def send(index, scheduled):
        event_id = str(uuid.uuid4())
        actual_start = time.monotonic()
        body = json.dumps({"eventId": event_id, "eventType": "performance.sample", "schemaVersion": 1,
                           "occurredAt": datetime.now(timezone.utc).isoformat(),
                           "attributes": {"platform": "synthetic-load"}}, separators=(",", ":")).encode()
        try:
            status = call(args.collector_url.rstrip("/") + "/v1/events", token, args.timeout, body)
            completed = time.monotonic()
            record = {"index": index, "event_id": event_id, "status": status,
                      "scheduled_s": scheduled - start, "started_s": actual_start - start,
                      "completed_s": completed - start, "schedule_delay_s": actual_start - scheduled,
                      "http_elapsed_s": completed - actual_start,
                      "scheduled_to_response_s": completed - scheduled}
            with lock:
                results.append(record)
        finally:
            slots.release()

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for index in range(planned):
            scheduled = start + index / args.rate
            remaining = scheduled - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            if slots.acquire(blocking=False):
                pool.submit(send, index, scheduled)
            else:
                with lock:
                    results.append({"index": index, "status": "client_capacity_drop",
                                    "scheduled_s": scheduled - start})
    load_finished = time.monotonic()
    accepted = {r["event_id"]: r for r in results if r["status"] == 202}
    ambiguous = {r["event_id"]: r for r in results if r["status"] == "transport_error"}
    pending = {**accepted, **ambiguous}
    visible = {}
    query_statuses = Counter()
    deadline = time.monotonic() + args.settle

    def observe(item):
        event_id, record = item
        status = call(args.worker_url.rstrip("/") + "/v1/events/" + event_id, token, args.timeout)
        return event_id, status, time.monotonic() - start

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        while pending and time.monotonic() < deadline:
            # At most one bounded sweep is active; executor work cannot grow without bound.
            items = list(pending.items())
            for offset in range(0, len(items), args.concurrency):
                batch = items[offset:offset + args.concurrency]
                for event_id, status, observed_s in pool.map(observe, batch):
                    query_statuses[str(status)] += 1
                    if status == 200:
                        record = pending.pop(event_id)
                        visible[event_id] = observed_s - record["scheduled_s"]
                if time.monotonic() >= deadline:
                    break
            if pending:
                time.sleep(0.2)
    missing = sorted(set(accepted) - visible.keys())
    raw = args.output / "requests.jsonl"
    with raw.open("w") as output:
        for record in sorted(results, key=lambda r: r["index"]):
            if record.get("event_id") in visible:
                record["observed_visibility_upper_bound_s"] = visible[record["event_id"]]
            output.write(json.dumps(record, sort_keys=True) + "\n")
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        revision, dirty = "unknown", True
    statuses = Counter(str(r["status"]) for r in results)
    successes = list(accepted.values())
    summary = {
        "started_at_utc": start_wall, "revision": revision, "working_tree_dirty": dirty,
        "generator": {"python": platform.python_version(), "os": platform.system(),
                      "rate_per_s": args.rate, "scheduled_duration_s": args.duration,
                      "concurrency": args.concurrency, "request_timeout_s": args.timeout},
        "scheduled": planned, "outcomes": dict(statuses),
        "load_and_inflight_drain_s": load_finished - start,
        "acknowledged_per_s_including_inflight_drain": len(accepted) / (load_finished - start),
        "successful_http_elapsed": quantiles([r["http_elapsed_s"] for r in successes]),
        "successful_scheduled_to_response": quantiles([r["scheduled_to_response_s"] for r in successes]),
        "generator_schedule_delay": quantiles([r["schedule_delay_s"] for r in results if "schedule_delay_s" in r]),
        "observed_visibility_upper_bound": quantiles([visible[event_id] for event_id in accepted if event_id in visible]),
        "accepted_reconciliation": {"accepted": len(accepted), "visible": len(accepted) - len(missing),
                                    "unresolved_ids": missing, "passed": bool(accepted) and not missing},
        "ambiguous_transport_outcomes": {"count": len(ambiguous),
                                         "later_visible": sum(event_id in visible for event_id in ambiguous)},
        "query_outcomes": dict(query_statuses),
        "interpretation": "Synthetic HTTP experiment. Visibility includes polling/sweep delay. No production capacity claim."
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["accepted_reconciliation"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
