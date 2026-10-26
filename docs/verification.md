# Verification evidence

Observed 25 October 2026. Exact tested source and suite counts are in
[`evidence/verification.json`](../evidence/verification.json). The implementation is a review candidate with explicit
pending deployment checks, not a production-readiness certification.

| Layer | Observed outcome |
| --- | --- |
| Java build | Full six-module Maven build passed on Temurin 21.0.13+9, Maven 3.9.12 |
| Unit/contracts | 66 passed, zero failures/errors/skips: core 27, config 8, buffer 12, observability 1, schemas 5, service 13 |
| Core compiler | 27 shared contract cases passed with Java 17 release target, all compiler warnings treated as errors |
| Packaged HTTP collector | Real Kafka/registry/volume test passed: JWT/scope/body validation, trace propagation, bounded buffering, JVM SIGKILL and accepted-ID replay |
| Kafka replication | Real three-broker test passed: leader failure, new leader, ISR loss and rejection below minISR 2; one controller, no controller-HA claim |
| Schema Registry | Separate Confluent 8.2 JVM test passed: compatible v1/v2 registration, breaking change rejected, registry kill/restart and stable schema IDs |
| Helm | Development chart lint/render passed; ten production guardrail cases passed |
| Kubernetes schema | 14 development/dependency/storage resources and eight production-rendered resources validated, zero errors/skips |
| Formatting/scripts | Google Java Format 1.28.0 check, Python compilation and shell syntax passed |
| AWS source | Terraform formatting passed; provider process failed to launch, so validation and two mocked policy runs remain unverified |
| PostgreSQL integration | Ten-container-backed test cases compile; execution pending because Docker is unavailable |
| Container and kind | Build, Compose runtime and Kubernetes smoke checks are written in CI but have not run |
| End-to-end load/JFR | Bounded harness and profiling protocol provided; no measured pipeline capacity or whole-service profiling result |
| Review/publication | README-only main is initialized; feature publication and draft PR are in progress. CI and independent review remain pending |

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
volume semantics or a successful rollout. The AWS provider archive matched the
publisher checksum and extracted package hash; the verified process still failed
to start in this environment. No checksum check was disabled, no mock test was
reported as passing and no AWS resource was created.

## Remaining integration gates

The Docker suite covers full HTTP-to-PostgreSQL persistence, accepted-ID set
reconciliation, duplicate/collision handling, an injected DB-commit/offset-commit
gap, real poison/DLT inspection, database pause with source-offset verification,
retention, broker pause with buffer saturation/recovery and intake drain.
It must pass on a Docker-capable runner before claiming these outcomes.

Run `./mvnw -Pintegration verify`, then the CI deployment checks and
`./scripts/kind-smoke.sh`. CI also resolves the separate registry runtime; tests
never silently skip when Docker is absent. The short kind load is a correctness
smoke, not a capacity benchmark. Preserve reports on the actual feature commit
and investigate failures before promoting this project in an application.
