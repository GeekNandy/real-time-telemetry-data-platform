#!/usr/bin/env bash
# An isolated Compose correctness smoke using synthetic data and generated credentials.
set -euo pipefail
cd "$(dirname "$0")/.."
telemetry_project="telemetry-compose-ci-${RANDOM}"
telemetry_reports="${TELEMETRY_COMPOSE_REPORTS:-data/compose-smoke}"
telemetry_secrets="$(mktemp -d)"
telemetry_compose=(docker compose -p "$telemetry_project" --env-file "$telemetry_secrets/env")
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
for name,value in {'key':key,'env':'DEMO_JWT_SECRET='+key+'\nDATABASE_PASSWORD='+secrets.token_hex(24)+'\n'}.items():
 f=p/name;f.write_text(value);f.chmod(0o600)
PY_SECRET
"${telemetry_compose[@]}" up -d --build
python3 scripts/wait-ready.py
telemetry_wal_permissions="$("${telemetry_compose[@]}" exec -T collector stat -c '%u:%g %a' /app/data/wal)"
printf '%s\n' "$telemetry_wal_permissions" > "$telemetry_reports/wal-permissions.txt"
[ "$telemetry_wal_permissions" = "10001:10001 700" ]
python3 - "$telemetry_secrets/key" "$telemetry_secrets/token" <<'PY_TOKEN'
from pathlib import Path
import base64,hashlib,hmac,json,sys,time
def enc(value):return base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).decode().rstrip('=')
now=int(time.time())
message=enc({'alg':'HS256','typ':'JWT'})+'.'+enc({'iss':'http://localhost:8080','aud':['telemetry'],'sub':'compose-ci','tenant_id':'compose-ci','scope':'telemetry.write telemetry.read','iat':now,'exp':now+600})
token=message+'.'+base64.urlsafe_b64encode(hmac.new(Path(sys.argv[1]).read_bytes(),message.encode(),hashlib.sha256).digest()).decode().rstrip('=')
target=Path(sys.argv[2]);target.write_text(token);target.chmod(0o600)
PY_TOKEN
python3 scripts/smoke-reconcile.py --token-file "$telemetry_secrets/token" --output "$telemetry_reports/http-contract"
