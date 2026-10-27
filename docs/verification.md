# Verification evidence

Verification results are tied to a tested source revision and execution environment.

| Scope | Tested source | Passing run |
| --- | --- | --- |
| Platform contracts, integrations and deployment smokes | `1e51d882e394` | [CI 40362033941](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40362033941) |
| Operator console, client contracts and browser workflows | `411b30e7c6d9` | [CI 40401774419](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40401774419) |

The [CI run index](../evidence/verification-runs.json) records selected passing
runs with their tested source revisions. Detailed reports are published as
workflow artifacts.

## Verification gates

| Layer | Gate |
| --- | --- |
| Java unit/contracts | 73 cases: core 27, config 8, buffer 14, schemas 10, observability 1, service 13 |
| Native integrations | Packaged HTTP/SIGKILL/replay, three-broker leader/minISR fault and real registry compatibility/restart |
| PostgreSQL integrations | Ten real Docker-backed scenarios, including ID reconciliation, tenant scope, replay, commit gap, poison/DLT, dependency outages, retention and drain |
| Registry transport regressions | Five actual HTTP cases, including headers followed by a stalled body and bounded oversized responses |
| Buffer regressions | Retry reclamation after a durable checkpoint and already-absent delivered files without stranding the queue |
| Startup polling | Two Python contracts for transient resets and the never-ready deadline |
| Experiment client | Six Python contracts: valid reconciliation, missing IDs, mismatched query IDs, invalid receipts, truncated responses and invalid settings |
| Deployment contracts | Helm lint, ten production guardrail cases and Kubernetes schema validation |
| Compose runtime | Build/start both roles, private spool ownership/mode, twenty accepted and queryable event IDs |
| Kubernetes runtime | Build image, rollout both roles in kind, private spool ownership/mode, twenty accepted/queryable IDs and collector restart |
| AWS reference | Terraform formatting, provider validation and two mocked policy runs; no AWS credentials or resource creation |

The Java build runs 86 distinct tests: 73 unit/contracts plus thirteen integration
cases. The separate native core runner repeats the same 27 core contracts and
must not be counted again. CI publishes Surefire/Failsafe and deployment reports.

The load client and container smokes validate response identity as well as HTTP
status. Twenty-event deployment workloads establish correctness, not capacity.
No end-to-end throughput, whole-service JFR finding, real AWS deployment,
controller-quorum/AZ HA, restore result or independent human approval is claimed.

Saved records retain their tested source revisions and observation dates. See the
[evidence index](../evidence/README.md) for archived reports and revision mappings.

## Operator console extension

The operator-console change adds seven Java HTTP/metrics contracts, bringing the
complete Java suite to 93 cases, plus eight Node client contracts and eight
Chromium fixture workflows. Its Compose smoke also performs a real browser
submission and matching PostgreSQL-backed lookup, verifies the fixed proxy
allowlist and observer authorization, and checks automatic Grafana provisioning
and both authenticated Prometheus scrapes. Existing twenty-event Compose and
kind checks remain required. See [the console guide](console.md) for exact scope.
The console PR's workflow and artifacts provide its own execution status;
fixture screenshots are not deployment or capacity evidence.
