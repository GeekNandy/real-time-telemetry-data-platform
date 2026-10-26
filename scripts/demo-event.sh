#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
demo_token="$(python3 scripts/demo-token.py --tenant demo)"
event_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
occurred_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
curl --fail-with-body --silent --show-error http://127.0.0.1:8080/v1/events \
  -H "Authorization: Bearer ${demo_token}" -H 'Content-Type: application/json' \
  -d "{\"eventId\":\"${event_id}\",\"eventType\":\"session.started\",\"schemaVersion\":1,\"occurredAt\":\"${occurred_at}\",\"attributes\":{\"platform\":\"linux\"}}"
printf '\n'
printf 'Retrieve after persistence: GET /v1/events/%s with a demo token\n' "${event_id}"
