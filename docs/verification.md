# Verification evidence

The foundation was reintroduced through sequential PRs, each based on its
predecessor's merged main and merged only after its checks passed. Source and
merge Git trees were compared at every step. See
[the machine-readable record](../evidence/review-sequence.json).

| Step | Merged PR | Tested source | Passing run |
| --- | --- | --- | --- |
| 1 | [#4](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/4) | `c9348ecbb21c` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40345796709) |
| 2 | [#5](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/5) | `d07a14ed83f4` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40347098253) |
| 3 | [#6](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/6) | `b4c0b5095e77` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40348037167) |
| 4 | [#7](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/7) | `15a77ee6803c` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40349646799) |
| 5 | [#8](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/8) | `4e74a38369ec` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40351688418) |
| 6 | [#9](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/9) | `70df3502533e` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40358095968) |
| 7 | [#10](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/10) | `671a6e86b544` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40359885256) |

The final tooling PR runs the following gates again on its own revision:

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

Historical evidence from PR #2 remains under evidence with its original source
revisions and observation dates. It is not evidence that a later revision passed.

## Operator console extension

The completed foundation's eighth PR is
[#11](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/11),
with [passing run 40362033941](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40362033941).
That run establishes the 86-case foundation result described above.

The operator-console change adds five Java HTTP/metrics contracts, bringing the
complete Java suite to 91 cases, plus eight Node client contracts and eight
Chromium fixture workflows. Its Compose smoke also performs a real browser
submission and matching PostgreSQL-backed lookup, verifies the fixed proxy
allowlist and observer authorization, and checks automatic Grafana provisioning
and both authenticated Prometheus scrapes. Existing twenty-event Compose and
kind checks remain required. See [the console guide](console.md) for exact scope.
The console PR's workflow and artifacts provide its own execution status;
fixture screenshots are not deployment or capacity evidence.
