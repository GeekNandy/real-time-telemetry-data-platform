# Verification evidence

Local JVM and schema results were first observed on 25 October 2026. On
26 October, all three GitHub CI jobs passed against the published implementation.
The exact source, execution revision, job IDs and outcomes are recorded in
[`evidence/ci-2026-10-26.json`](../evidence/ci-2026-10-26.json). The original local
record remains in [`evidence/verification.json`](../evidence/verification.json).

| Layer | Observed outcome |
| --- | --- |
| Java build | Full six-module Maven integration build passed on GitHub CI |
| Unit/contracts | 66 passed, zero failures/errors/skips: core 27, config 8, buffer 12, observability 1, schemas 5, service 13 |
| Core compiler | 27 shared cases passed with Java 17 release target and compiler warnings treated as errors |
| Native integrations | Three passed: packaged HTTP collector buffering/SIGKILL/replay, broker leader/ISR failure, and registry compatibility/restart |
| PostgreSQL integration | All ten Docker-backed cases passed: exact ID reconciliation, tenant retrieval, duplicates, commit gap, collision/DLT, poison, database outage, retention, Kafka outage and drain |
| Helm/Kubernetes schema | Chart lint, ten production guardrail cases, and development/production resource schema validation passed |
| AWS source | Terraform formatting, provider validation and two mocked policy runs passed on GitHub CI; no AWS resources were created |
| Container/kind | Application image built; collector/worker became ready; private spool permissions were `10001:10001 700`; all 20 offered requests returned HTTP 202 and became queryable; collector restart completed |
| Compose | Configuration validation passed; the full Compose runtime demonstration has not been separately executed |
| Formatting/scripts | Java formatting, Python compilation and shell syntax passed |
| Capacity/JFR | No measured pipeline capacity or whole-service profiling result is claimed |
| Review/publication | Main contains only README.md; the implementation is published in [draft PR #2](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/2), which remains unmerged and awaits independent review |

The kind workload is a twenty-request correctness smoke, not a performance
benchmark. Raw synthetic request outcomes are preserved in
[`evidence/ci-smoke-2026-10-26-requests.jsonl`](../evidence/ci-smoke-2026-10-26-requests.jsonl).
The source and PR merge execution revisions have identical Git trees.

## Published source

Publication completed on 26 October 2026. The original tested content is preserved
in the published commit with the same Git tree. See
[`evidence/published-source.json`](../evidence/published-source.json) for the
original-to-published commit mapping and byte-for-byte tree checks. Historical
test results retain their original observation date; publication is not a new
test execution. GitHub CI reports subsequent runs on the PR.

## Executed JVM checks

```bash
./scripts/verify-core.sh
./mvnw --batch-mode --no-transfer-progress verify
./mvnw --batch-mode --no-transfer-progress \
  -f tools/schema-registry-runtime/pom.xml \
  org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath \
  -Dmdep.outputFile=target/classpath.txt
./mvnw --batch-mode --no-transfer-progress -Pregistry-integration verify
```

The authoring runtime used the same Maven version through a verified local
installation with its runtime-provided HTTPS proxy and system CA trust store.
The final native suite executed all three integration classes, with no skips.
The packaged collector starts a separate JVM from the runnable JAR plus runtime
libraries. It does not use a mocked HTTP server or database substitute.

The core JDK runner and JUnit adapter execute the same 27 cases and must not be
counted twice. Its randomized replay case uses seed 42, 500 unique IDs and 1,200
in-memory deliveries. The buffer suite includes a real child-JVM termination,
checksummed corruption, ownership lock, recovery and quota checks.

## Deployment checks

```bash
helm lint deploy/helm/telemetry -f deploy/helm/telemetry/values-ci.yaml
helm template ci deploy/helm/telemetry -f deploy/helm/telemetry/values-ci.yaml > /tmp/telemetry.yaml
kubeconform -strict -summary /tmp/telemetry.yaml deploy/kind/dependencies.yaml deploy/kubernetes/storageclass-aws.yaml
python3 scripts/check-production-chart.py
terraform -chdir=infra/aws fmt -check
```

Schema validation proves API structure, not scheduling, network-policy enforcement,
volume semantics or a successful rollout. The original authoring environment could not launch the publisher-verified AWS
provider. GitHub CI subsequently validated the configuration and passed both
mocked policy runs. No checksum check was disabled and no AWS resource was created.

## Remaining validation gates

The Docker and kind correctness gates have passed. The remaining work is an
independent review, a separate Compose runtime demonstration if that path is to
be advertised, and a properly controlled end-to-end load/profiling experiment
before making capacity claims. A real AWS deployment, HA exercise and restore
exercise are not established by Terraform mocks or a single-node kind cluster.

Reproduce the Java integration suite with `./mvnw -Pintegration verify` after
resolving the separate registry runtime, then run the deployment contract checks
and `./scripts/kind-smoke.sh`. These tests do not silently skip when Docker is
absent. Preserve reports on the actual source revision and investigate failures
before changing the evidence record.
