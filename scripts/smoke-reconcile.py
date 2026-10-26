#!/usr/bin/env python3
"""A fixed twenty-event deployment smoke; it does not measure pipeline capacity."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
import uuid


def request(url, token, body=None):
    value = urllib.request.Request(url, data=body, headers={
        'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(value, timeout=3) as response:
            data = response.read(65537)
            if len(data) > 65536:
                return 'invalid_response', None
            return response.status, json.loads(data)
    except urllib.error.HTTPError as failure:
        with failure:
            failure.read(65536)
        return failure.code, None
    except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError):
        return 'transport_or_response_error', None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collector-url', default='http://127.0.0.1:8080')
    parser.add_argument('--worker-url', default='http://127.0.0.1:8082')
    parser.add_argument('--token-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    token = args.token_file.read_text().strip()
    if not token or len(token) > 16384:
        parser.error('Token file is empty or oversized')
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    accepted = set()
    for index in range(20):
        event_id = str(uuid.uuid4())
        body = json.dumps({'eventId': event_id, 'eventType': 'performance.sample',
                           'schemaVersion': 1, 'occurredAt': datetime.now(timezone.utc).isoformat(),
                           'attributes': {'platform': 'synthetic-smoke'}}).encode()
        status, receipt = request(args.collector_url.rstrip('/') + '/v1/events', token, body)
        if status == 202:
            if not isinstance(receipt, dict) or receipt.get('eventId') != event_id:
                status = 'invalid_receipt'
            else:
                accepted.add(event_id)
        results.append({'index': index, 'event_id': event_id, 'status': status})
    pending = set(accepted)
    deadline = time.monotonic() + 60
    while pending and time.monotonic() < deadline:
        for event_id in list(pending):
            if time.monotonic() >= deadline:
                break
            status, row = request(args.worker_url.rstrip('/') + '/v1/events/' + event_id, token)
            if status == 200 and isinstance(row, dict) and row.get('eventId') == event_id:
                pending.remove(event_id)
        if pending:
            time.sleep(0.2)
    summary = {
        'scheduled': 20,
        'outcomes': dict(Counter(str(row['status']) for row in results)),
        'accepted_reconciliation': {
            'accepted': len(accepted), 'visible': len(accepted) - len(pending),
            'unresolved_ids': sorted(pending), 'passed': len(accepted) == 20 and not pending},
        'interpretation': 'Twenty-event deployment correctness smoke; no capacity result',
    }
    (args.output / 'requests.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in results))
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    return 0 if summary['outcomes'] == {'202': 20} and summary['accepted_reconciliation']['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
