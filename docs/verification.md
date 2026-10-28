# Verification evidence

Verification results identify the tested source, execution revision and environment.
The Kafka-confirmed runtime passed [CI 40579394488](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40579394488)
on source `556cfe22739b`. Its merged revision is `3876a0e0d816`; the executed and
merged trees both equal `6b9c68defa30ff4e9ec65ff8e4df4c8b8b43c822`.

All four required checks passed: client/browser contracts, Java integrations,
deployment contracts and Kubernetes smoke. The [runtime evidence record](../evidence/replicated-acceptance-2026-10-27.json)
contains suite counts, source mappings and deployment observations. Detailed
reports are available as workflow artifacts.

| Layer | Observed gate |
| --- | --- |
| Java unit/contracts | 92 cases, including bounded acknowledgement waiters and effective producer/topic policy |
| Native integrations | Three cases: packaged HTTP/SIGKILL/replay, broker leader/minISR faults and real registry compatibility/restart |
| PostgreSQL integrations | Twelve cases covering ID reconciliation, tenant scope, deduplication, commit gaps, poison/DLT, dependency outages, retention, drain, delayed acknowledgement and replicated collector-volume loss |
| Buffer recovery | Quotas, checksums, ownership, child-JVM recovery and retryable reclamation after a durable checkpoint |
| Registry transport | Five actual HTTP cases, including stalled bodies and oversized responses |
| Browser/client contracts | 24 Node cases; eight operator-browser, six recorded-explorer and two protected-access workflows |
| Startup and load clients | Two readiness retry contracts and six load/reconciliation contracts |
| Compose runtime | Three data brokers, RF3/minISR2, twenty accepted/queryable IDs, private spool ownership, actual browser submission and authenticated observability provisioning |
| Kubernetes runtime | Single development broker, rollout in kind, twenty accepted/queryable IDs, private spool ownership and collector restart |
| Deployment policy | Helm lint, ten production guardrails, Kubernetes schema checks and two mocked Terraform policy runs |

The Java reports contain **107 distinct tests: 92 unit/contracts and 15
integrations, with zero failures, errors or skips**. The standalone native core
runner repeats 27 existing cases and is not added to that count.

HTTP 202 follows Kafka acknowledgement and a durable delivery checkpoint. The
replicated integration removes the collector's local volume after 202, stops a
broker and reconciles the accepted identities against PostgreSQL. It also checks
refusal below minISR, cancellation, identical retries and replay across a real
checkpoint write failure. Each accepted identity has one downstream row.

The [hosted recording](../evidence/hosted-run-2026-10-27/manifest.json) retains
original report bytes and their SHA-256 digests. Compose uses three broker
replicas with minISR 2; kind uses one development broker. The two configurations
have different replication guarantees.

Twenty-event smokes establish correctness, not capacity. Controller-quorum/AZ
availability, real AWS deployment and restore exercises are outside these checks.
Owner merge authorization and automated checks do not establish independent human
review. See the [review guide](review-guide.md) for the review scope and the
[performance protocol](performance.md) for measured-workload requirements.

The [CI run index](../evidence/verification-runs.json) retains selected passing
source mappings. Read archived reports against their recorded revisions and
environments.
