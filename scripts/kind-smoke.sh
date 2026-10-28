#!/usr/bin/env bash
# Requires Docker, kind, kubectl, Helm, Java21 and Python3. Creates an isolated local cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
telemetry_cluster="telemetry-ci-${RANDOM}"
telemetry_context="kind-${telemetry_cluster}"
telemetry_namespace=telemetry
telemetry_dependencies=telemetry-deps
telemetry_reports="${TELEMETRY_REPORTS:-data/kind-smoke}"
mkdir -p "$telemetry_reports"
telemetry_forward_collector=''
telemetry_forward_worker=''
telemetry_roll_load=''
cleanup() {
  if [ -n "$telemetry_roll_load" ]; then kill "$telemetry_roll_load" 2>/dev/null || true; fi
  if [ -n "$telemetry_forward_collector" ]; then kill "$telemetry_forward_collector" 2>/dev/null || true; fi
  if [ -n "$telemetry_forward_worker" ]; then kill "$telemetry_forward_worker" 2>/dev/null || true; fi
  kubectl --context "$telemetry_context" -n "$telemetry_namespace" get pods,pvc > "$telemetry_reports/resources.txt" 2>&1 || true
  kubectl --context "$telemetry_context" -n "$telemetry_namespace" logs -l app.kubernetes.io/name=telemetry --all-containers --prefix > "$telemetry_reports/application.log" 2>&1 || true
  kubectl --context "$telemetry_context" -n "$telemetry_namespace" get events --sort-by=.lastTimestamp > "$telemetry_reports/events.txt" 2>&1 || true
  for telemetry_dependency in kafka registry postgres; do
    kubectl --context "$telemetry_context" -n "$telemetry_dependencies" logs -l "app=${telemetry_dependency}" --all-containers --prefix --tail=200 > "$telemetry_reports/${telemetry_dependency}.log" 2>&1 || true
    kubectl --context "$telemetry_context" -n "$telemetry_dependencies" logs -l "app=${telemetry_dependency}" --all-containers --prefix --previous --tail=200 > "$telemetry_reports/${telemetry_dependency}-previous.log" 2>&1 || true
  done
  kind delete cluster --name "$telemetry_cluster"
}
trap cleanup EXIT
kind create cluster --name "$telemetry_cluster" --config deploy/kind/cilium.yaml
python3 scripts/install-cilium.py --context "$telemetry_context"
kubectl --context "$telemetry_context" wait --for=condition=Ready nodes --all --timeout=180s
kubectl --context "$telemetry_context" create namespace "$telemetry_namespace"
kubectl --context "$telemetry_context" label namespace "$telemetry_namespace" pod-security.kubernetes.io/enforce=restricted pod-security.kubernetes.io/enforce-version=latest
kubectl --context "$telemetry_context" create namespace "$telemetry_dependencies"
kubectl --context "$telemetry_context" create namespace telemetry-client
kubectl --context "$telemetry_context" label namespace telemetry-client purpose=telemetry-client pod-security.kubernetes.io/enforce=restricted
telemetry_secret_dir="$(mktemp -d)"
trap 'rm -rf "$telemetry_secret_dir"; cleanup' EXIT
python3 - "$telemetry_secret_dir" <<'PY'
from pathlib import Path
import secrets,sys
p=Path(sys.argv[1])
for name,value in {'database-password':secrets.token_hex(24),'demo-jwt-secret':secrets.token_hex(32),
 'database-url':'jdbc:postgresql://postgres.telemetry-deps:5432/telemetry?connectTimeout=2&socketTimeout=5&options=-c%20statement_timeout=3000',
 'database-user':'telemetry'}.items():
 f=p/name;f.write_text(value);f.chmod(0o600)
PY
kubectl --context "$telemetry_context" -n "$telemetry_namespace" create secret generic telemetry-runtime --from-file="$telemetry_secret_dir" >/dev/null
kubectl --context "$telemetry_context" -n "$telemetry_dependencies" create secret generic telemetry-runtime --from-file=database-password="$telemetry_secret_dir/database-password" >/dev/null
kubectl --context "$telemetry_context" -n "$telemetry_dependencies" apply -f deploy/kind/dependencies.yaml
kubectl --context "$telemetry_context" -n "$telemetry_dependencies" rollout status statefulset/kafka --timeout=240s
kubectl --context "$telemetry_context" -n "$telemetry_dependencies" rollout status statefulset/postgres --timeout=120s
kubectl --context "$telemetry_context" -n "$telemetry_dependencies" rollout status deployment/registry --timeout=240s
docker build -t real-time-telemetry-data-platform:ci .
kind load docker-image --name "$telemetry_cluster" real-time-telemetry-data-platform:ci
helm upgrade --install ci deploy/helm/telemetry --kube-context "$telemetry_context" -n "$telemetry_namespace" -f deploy/helm/telemetry/values-ci.yaml --wait --timeout 240s
python3 scripts/test-network-policy.py --context "$telemetry_context" --cluster "$telemetry_cluster" --output "$telemetry_reports/network-policy"
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
now=int(time.time());message=enc({'alg':'HS256','typ':'JWT'})+'.'+enc({'iss':'http://localhost:8080','aud':['telemetry'],'sub':'kind-ci','tenant_id':'kind-ci','scope':'telemetry.write telemetry.read','iat':now,'exp':now+1800})
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
# Reconcile accepted traffic while workers roll. Collector intake remains available.
python3 scripts/load.py --rate 5 --duration 30 --settle 180 --token-file "$telemetry_reports/token" --output "$telemetry_reports/worker-roll" > "$telemetry_reports/worker-roll.log" 2>&1 &
telemetry_roll_load=$!
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout restart deployment/ci-telemetry-worker
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status deployment/ci-telemetry-worker --timeout=180s
kill "$telemetry_forward_worker" 2>/dev/null || true
wait "$telemetry_forward_worker" 2>/dev/null || true
kubectl --context "$telemetry_context" -n "$telemetry_namespace" port-forward service/ci-telemetry-worker 8082:8080 > "$telemetry_reports/worker-forward.log" 2>&1 &
telemetry_forward_worker=$!
wait "$telemetry_roll_load"
telemetry_roll_load=''
python3 - "$telemetry_reports/worker-roll/summary.json" <<'PY_ROLL'
import json,sys
from pathlib import Path
s=json.loads(Path(sys.argv[1]).read_text())
assert s['outcomes']=={'202':150},s['outcomes']
assert s['accepted_reconciliation']['passed'] and s['accepted_reconciliation']['visible']==150,s
PY_ROLL
# Deliberately bad image must leave old replicas available, then undo and verify.
kubectl --context "$telemetry_context" -n "$telemetry_namespace" set image deployment/ci-telemetry-worker worker=example.invalid/telemetry:synthetic-unavailable
if kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status deployment/ci-telemetry-worker --timeout=20s; then
  echo 'Expected deliberately invalid image rollout to fail' >&2
  exit 1
fi
kubectl --context "$telemetry_context" -n "$telemetry_namespace" get deployment/ci-telemetry-worker -o json > "$telemetry_reports/failed-rollout.json"
python3 - "$telemetry_reports/failed-rollout.json" <<'PY_AVAILABLE'
import json,sys
from pathlib import Path
s=json.loads(Path(sys.argv[1]).read_text())
assert s['status']['availableReplicas']>=2,s['status']
PY_AVAILABLE
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout undo deployment/ci-telemetry-worker
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status deployment/ci-telemetry-worker --timeout=180s
kubectl --context "$telemetry_context" -n "$telemetry_namespace" scale deployment/ci-telemetry-worker --replicas=3
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status deployment/ci-telemetry-worker --timeout=180s
kubectl --context "$telemetry_context" -n "$telemetry_namespace" scale deployment/ci-telemetry-worker --replicas=2
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status deployment/ci-telemetry-worker --timeout=180s
kill "$telemetry_forward_worker" 2>/dev/null || true
wait "$telemetry_forward_worker" 2>/dev/null || true
kubectl --context "$telemetry_context" -n "$telemetry_namespace" port-forward service/ci-telemetry-worker 8082:8080 > "$telemetry_reports/worker-forward.log" 2>&1 &
telemetry_forward_worker=$!
python3 scripts/wait-ready.py
python3 scripts/load.py --rate 5 --duration 4 --token-file "$telemetry_reports/token" --output "$telemetry_reports/after-rollback-scale"
python3 - "$telemetry_reports/after-rollback-scale/summary.json" <<'PY_RECOVERED'
import json,sys
from pathlib import Path
s=json.loads(Path(sys.argv[1]).read_text())
assert s['outcomes']=={'202':20} and s['accepted_reconciliation']['visible']==20,s
PY_RECOVERED
# Operational fault exercise; no Kubernetes capacity or multi-zone claim.
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout restart statefulset/ci-telemetry-collector
kubectl --context "$telemetry_context" -n "$telemetry_namespace" rollout status statefulset/ci-telemetry-collector --timeout=180s
kubectl --context "$telemetry_context" -n "$telemetry_namespace" get pvc -o json > "$telemetry_reports/persistent-volumes.json"
