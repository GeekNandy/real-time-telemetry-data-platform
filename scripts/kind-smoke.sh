#!/usr/bin/env bash
# Requires Docker, kind, kubectl, Helm, Java21 and Python3. Creates an isolated local cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
telemetry_cluster="telemetry-ci-${RANDOM}"
telemetry_context="kind-${telemetry_cluster}"
telemetry_namespace=telemetry
telemetry_reports="${TELEMETRY_REPORTS:-data/kind-smoke}"
mkdir -p "$telemetry_reports"
telemetry_forward_collector=''
telemetry_forward_worker=''
cleanup() {
  if [ -n "$telemetry_forward_collector" ]; then kill "$telemetry_forward_collector" 2>/dev/null || true; fi
  if [ -n "$telemetry_forward_worker" ]; then kill "$telemetry_forward_worker" 2>/dev/null || true; fi
  kubectl --context "$telemetry_context" -n "$telemetry_namespace" get pods,pvc > "$telemetry_reports/resources.txt" 2>&1 || true
  kubectl --context "$telemetry_context" -n "$telemetry_namespace" logs -l app.kubernetes.io/name=telemetry --all-containers --prefix > "$telemetry_reports/application.log" 2>&1 || true
  kubectl --context "$telemetry_context" -n "$telemetry_namespace" get events --sort-by=.lastTimestamp > "$telemetry_reports/events.txt" 2>&1 || true
  for telemetry_dependency in kafka registry postgres; do
    kubectl --context "$telemetry_context" -n "$telemetry_namespace" logs -l "app=${telemetry_dependency}" --all-containers --prefix --tail=200 > "$telemetry_reports/${telemetry_dependency}.log" 2>&1 || true
    kubectl --context "$telemetry_context" -n "$telemetry_namespace" logs -l "app=${telemetry_dependency}" --all-containers --prefix --previous --tail=200 > "$telemetry_reports/${telemetry_dependency}-previous.log" 2>&1 || true
  done
  kind delete cluster --name "$telemetry_cluster"
}
trap cleanup EXIT
kind create cluster --name "$telemetry_cluster" --wait 120s
kubectl --context "$telemetry_context" create namespace "$telemetry_namespace"
telemetry_secret_dir="$(mktemp -d)"
trap 'rm -rf "$telemetry_secret_dir"; cleanup' EXIT
python3 - "$telemetry_secret_dir" <<'PY'
from pathlib import Path
import secrets,sys
p=Path(sys.argv[1])
for name,value in {'database-password':secrets.token_hex(24),'demo-jwt-secret':secrets.token_hex(32),
 'database-url':'jdbc:postgresql://postgres:5432/telemetry?connectTimeout=2&socketTimeout=5&options=-c%20statement_timeout=3000',
 'database-user':'telemetry'}.items():
 f=p/name;f.write_text(value);f.chmod(0o600)
PY
kubectl --context "$telemetry_context" -n "$telemetry_namespace" create secret generic telemetry-runtime --from-file="$telemetry_secret_dir" >/dev/null
kubectl --context "$telemetry_context" -n "$telemetry_namespace" apply -f deploy/kind/dependencies.yaml
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status statefulset/kafka --timeout=240s
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status statefulset/postgres --timeout=120s
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status deployment/registry --timeout=240s
docker build -t real-time-telemetry-data-platform:ci .
kind load docker-image --name "$telemetry_cluster" real-time-telemetry-data-platform:ci
helm upgrade --install ci deploy/helm/telemetry --kube-context "$telemetry_context" -n "$telemetry_namespace" -f deploy/helm/telemetry/values-ci.yaml --wait --timeout 240s
# The process must own the private spool directory, not the Kubernetes-managed volume root.
telemetry_wal_permissions="$(kubectl --context "$telemetry_context" -n "$telemetry_namespace" exec statefulset/ci-telemetry-collector -- stat -c '%u:%g %a' /app/data/wal)"
printf '%s\n' "$telemetry_wal_permissions" > "$telemetry_reports/wal-permissions.txt"
[ "$telemetry_wal_permissions" = "10001:10001 700" ]
kubectl --context "$telemetry_context" -n "$telemetry_namespace" port-forward service/ci-telemetry-collector 8080:8080 > "$telemetry_reports/collector-forward.log" 2>&1 &
telemetry_forward_collector=$!
kubectl --context "$telemetry_context" -n "$telemetry_namespace" port-forward service/ci-telemetry-worker 8082:8080 > "$telemetry_reports/worker-forward.log" 2>&1 &
telemetry_forward_worker=$!
python3 scripts/wait-ready.py
python3 - "$telemetry_secret_dir/demo-jwt-secret" "$telemetry_reports/token" <<'PY'
import base64,hashlib,hmac,json,time,sys
from pathlib import Path
def enc(x):return base64.urlsafe_b64encode(json.dumps(x,separators=(',',':')).encode()).decode().rstrip('=')
now=int(time.time());message=enc({'alg':'HS256','typ':'JWT'})+'.'+enc({'iss':'http://localhost:8080','aud':['telemetry'],'sub':'kind-ci','tenant_id':'kind-ci','scope':'telemetry.write telemetry.read','iat':now,'exp':now+600})
value=message+'.'+base64.urlsafe_b64encode(hmac.new(Path(sys.argv[1]).read_bytes(),message.encode(),hashlib.sha256).digest()).decode().rstrip('=')
p=Path(sys.argv[2]);p.write_text(value);p.chmod(0o600)
PY
trap 'rm -f "$telemetry_reports/token"; rm -rf "$telemetry_secret_dir"; cleanup' EXIT
python3 scripts/load.py --rate 5 --duration 4 --token-file "$telemetry_reports/token" --output "$telemetry_reports/http-contract"
python3 - "$telemetry_reports/http-contract/summary.json" <<'PY_SMOKE'
import json,sys
from pathlib import Path
summary=json.loads(Path(sys.argv[1]).read_text())
assert summary['scheduled']==20, summary
assert summary['outcomes']=={'202':20}, summary['outcomes']
assert summary['accepted_reconciliation']['passed'], summary['accepted_reconciliation']
assert summary['accepted_reconciliation']['visible']==20, summary['accepted_reconciliation']
PY_SMOKE
# This is a rollout/readiness/volume-mount smoke check, not a Kubernetes capacity result.
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout restart statefulset/ci-telemetry-collector
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status statefulset/ci-telemetry-collector --timeout=180s
kubectl --context "$telemetry_context" -n "$telemetry_namespace" get pvc -o json > "$telemetry_reports/persistent-volumes.json"
