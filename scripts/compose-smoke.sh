#!/usr/bin/env bash
# An isolated Compose correctness smoke using synthetic data and generated credentials.
set -euo pipefail
cd "$(dirname "$0")/.."
telemetry_project="telemetry-compose-ci-${RANDOM}"
telemetry_reports="${TELEMETRY_COMPOSE_REPORTS:-data/compose-smoke}"
telemetry_secrets="$(mktemp -d)"
telemetry_compose=(docker compose -f compose.yaml -f compose.replicated.yaml -p "$telemetry_project" --env-file "$telemetry_secrets/env" --profile console --profile observability)
mkdir -p "$telemetry_reports"
cleanup() {
  if [ -f "$telemetry_secrets/env" ]; then
    "${telemetry_compose[@]}" logs --no-color --tail=200 > "$telemetry_reports/application.log" 2>&1 || true
    "${telemetry_compose[@]}" ps --all > "$telemetry_reports/resources.txt" 2>&1 || true
    "${telemetry_compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$telemetry_secrets"
}
trap cleanup EXIT
python3 - "$telemetry_secrets" <<'PY_SECRET'
from pathlib import Path
import secrets,sys
p=Path(sys.argv[1])
key=secrets.token_hex(32)
(p/'auth').mkdir(mode=0o700)
for name,value in {'key':key,'env':'DEMO_JWT_SECRET='+key+'\nDATABASE_PASSWORD='+secrets.token_hex(24)+'\nTELEMETRY_AUTH_DIR='+str(p/'auth')+'\n'}.items():
 f=p/name;f.write_text(value);f.chmod(0o600)
password=p/'auth/grafana-admin-password';password.write_text(secrets.token_urlsafe(24));password.chmod(0o644)
PY_SECRET
python3 scripts/demo-token.py --env-file "$telemetry_secrets/env" --observe > "$telemetry_secrets/auth/observe.token"
chmod 644 "$telemetry_secrets/auth/observe.token"
"${telemetry_compose[@]}" up -d --build
python3 scripts/wait-ready.py
telemetry_wal_permissions="$("${telemetry_compose[@]}" exec -T collector stat -c '%u:%g %a' /app/data/wal)"
printf '%s\n' "$telemetry_wal_permissions" > "$telemetry_reports/wal-permissions.txt"
[ "$telemetry_wal_permissions" = "10001:10001 700" ]
python3 scripts/demo-token.py --env-file "$telemetry_secrets/env" --tenant compose-ci --console > "$telemetry_secrets/token"
chmod 600 "$telemetry_secrets/token"
python3 scripts/load.py --rate 5 --duration 4 --token-file "$telemetry_secrets/token" --output "$telemetry_reports/http-contract"
python3 - "$telemetry_reports/http-contract/summary.json" <<'PY_SMOKE'
import json,sys
from pathlib import Path
summary=json.loads(Path(sys.argv[1]).read_text())
assert summary['scheduled']==20, summary
assert summary['outcomes']=={'202':20}, summary['outcomes']
assert summary['accepted_reconciliation']['passed'], summary['accepted_reconciliation']
assert summary['accepted_reconciliation']['visible']==20, summary['accepted_reconciliation']
PY_SMOKE
python3 scripts/check-observability.py --password-file "$telemetry_secrets/auth/grafana-admin-password" --output "$telemetry_reports/observability.json"
TELEMETRY_CONSOLE_TOKEN_FILE="$telemetry_secrets/token" TELEMETRY_BROWSER_REPORTS="$telemetry_reports/console" node console/test/live.mjs

python3 scripts/test-postgres-restore.py --project "$telemetry_project" --env-file "$telemetry_secrets/env" --output "$telemetry_reports/restore.json"
