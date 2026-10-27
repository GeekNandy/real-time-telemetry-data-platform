# Technical review guide

Review changes against the platform's delivery, resource and security contracts.

| Area | Review focus | Reference |
| --- | --- | --- |
| Admission and concurrency | Atomic limits, immutable events, cancellation and drain | [Design](design.md) |
| Durable intake | Synchronized append, exclusive spool ownership and checkpoint recovery | [Buffer recovery](buffer.md) |
| Schema lifecycle | Compatible envelopes and bounded registry transport | [Schema contracts](schemas.md) |
| Persistence | Transaction/offset ordering, idempotency, tenant isolation and retention | [Worker operations](worker.md) |
| Deployment | Separate workloads, retained volumes, transport policy and network boundaries | [Operations](operations.md) |
| Performance | Offered versus accepted traffic, bounded clients and accepted-ID reconciliation | [Profiling protocol](performance.md) |
| Verification | Relevant contracts, fault checks and results tied to source revisions | [Verification evidence](verification.md) |

HTTP acceptance follows a synchronized local-volume append. Recovery depends on
retaining that volume until Kafka acknowledgement. Replicated acceptance is
tracked separately in [issue #3](https://github.com/GeekNandy/real-time-telemetry-data-platform/issues/3).

Use the [contribution workflow](../CONTRIBUTING.md) for branch, check and merge requirements.
