#!/usr/bin/env bash
# Isolated single-host KRaft leader-failover correctness exercise. No cloud resources.
set -euo pipefail
cd "$(dirname "$0")/.."

project="telemetry-kraft-smoke-$RANDOM-$RANDOM"
reports="${TELEMETRY_KRAFT_REPORTS:-data/kraft-quorum-smoke}"
scratch="$(mktemp -d)"
mkdir -p "$reports"
compose=(docker compose -p "$project" -f compose.yaml -f compose.replicated.yaml -f compose.kraft-quorum.yaml --env-file "$scratch/env")

cleanup() {
  if [[ -f "$scratch/env" ]]; then
    "${compose[@]}" ps --all > "$reports/containers.txt" 2>&1 || true
    "${compose[@]}" logs --no-color --tail=200 > "$reports/containers.log" 2>&1 || true
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$scratch"
}
trap cleanup EXIT

python3 - "$scratch" <<'PY_SECRETS'
from pathlib import Path
import secrets
import sys
directory = Path(sys.argv[1])
settings = directory / "env"
settings.write_text(
    "DEMO_JWT_SECRET=" + secrets.token_hex(32) + "\n"
    "DATABASE_PASSWORD=" + secrets.token_hex(24) + "\n"
)
settings.chmod(0o600)
PY_SECRETS

# The -p project name gives every controller and broker a fresh, isolated volume.
python3 scripts/test-kraft-quorum.py
# Bring up storage and transport first. The collector checks actual replication/ISR
# at startup and must fail closed, not race Kafka's first topic propagation.
"${compose[@]}" up -d --build kafka kafka2 kafka3 registry postgres jaeger

kafka_cli() {
  timeout 20s "${compose[@]}" exec -T kafka "/opt/kafka/bin/$1" "${@:2}"
}
for topic in telemetry.events.v1 telemetry.events.v1.dlt; do
  kafka_cli kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 \
    --replication-factor 3 --config min.insync.replicas=2 \
    --config retention.ms=604800000 --config unclean.leader.election.enable=false

  ready=false
  for _ in $(seq 1 40); do
    if kafka_cli kafka-topics.sh --bootstrap-server localhost:9092 \
      --describe --topic "$topic" 2>/dev/null |
      python3 -c 'import re, sys
parts = [line for line in sys.stdin if "Partition:" in line]
sys.exit(0 if len(parts) == 3 and all(
  (m := re.search(r"Isr: ([0-9,]+)", p)) and len(m.group(1).split(",")) >= 2
  for p in parts
) else 1)'; then
      ready=true
      break
    fi
    sleep 2
  done
  if [[ "$ready" != true ]]; then
    echo "Topic $topic failed to reach minISR=2" >&2
    exit 1
  fi
done
"${compose[@]}" up -d collector worker
python3 scripts/wait-ready.py
python3 scripts/demo-token.py --env-file "$scratch/env" --tenant kraft-smoke --console > "$scratch/token"
chmod 600 "$scratch/token"

status() {
  timeout 20s "${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
}
leader_from_status() {
  awk '$1 == "LeaderId:" {print $2}'
}
wait_for_leader() {
  local expected_old="${1:-}"
  local output leader
  for _ in $(seq 1 40); do
    if output="$(status 2>/dev/null)"; then
      leader="$(printf '%s\n' "$output" | leader_from_status)"
      case "$leader" in
        101|102|103)
          if [[ -z "$expected_old" || "$leader" != "$expected_old" ]]; then
            printf '%s\n' "$output"
            return 0
          fi
          ;;
      esac
    fi
    sleep 2
  done
  echo 'KRaft controller quorum did not elect the expected leader within the bound' >&2
  return 1
}

verify_receipts() {
  local phase="$1"
  python3 scripts/load.py --rate 5 --duration 4 --token-file "$scratch/token" \
    --output "$reports/$phase"
  python3 - "$reports/$phase/summary.json" <<'PY_RECEIPTS'
from pathlib import Path
import json
import sys
summary = json.loads(Path(sys.argv[1]).read_text())
assert summary["scheduled"] == 20, summary
assert summary["outcomes"] == {"202": 20}, summary["outcomes"]
assert summary["accepted_reconciliation"]["passed"], summary["accepted_reconciliation"]
assert summary["accepted_reconciliation"]["visible"] == 20, summary["accepted_reconciliation"]
PY_RECEIPTS
}

wait_for_leader > "$reports/quorum-before.txt"
old_leader="$(leader_from_status < "$reports/quorum-before.txt")"
verify_receipts before-failure

# Crash the active metadata leader, not the controller volumes or brokers.
leader_service="kafka-controller-$((old_leader - 100))"
"${compose[@]}" kill -s SIGKILL "$leader_service"
wait_for_leader "$old_leader" > "$reports/quorum-during-failure.txt"
verify_receipts during-failure

# Rejoin using the retained persistent metadata volume.
"${compose[@]}" start "$leader_service"
wait_for_leader > "$reports/quorum-recovered.txt"
printf 'Previous leader: %s\nFailover leader: %s\n' \
  "$old_leader" "$(leader_from_status < "$reports/quorum-during-failure.txt")" \
  > "$reports/leader-transition.txt"
echo "KRaft single-controller failover and acknowledged-ID reconciliation passed (single-host simulation)."
