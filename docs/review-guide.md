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

HTTP acceptance follows Kafka acknowledgement and a durable delivery checkpoint.
Review timeout/cancellation ambiguity, effective producer/topic policy, retained
unconfirmed spool records and deduplicated replay. The three-broker reference and
the single-broker development configuration have different replication guarantees.

Use the [contribution workflow](../CONTRIBUTING.md) for branch, check and merge requirements.

## Independent review record

An independent human reviewer should record their identity, reviewed source/tree,
date and verdict. Review the 202 acknowledgement boundary, waiter/admission limits,
timeout/cancellation ambiguity, checkpoint/replay ordering, effective broker policy,
tenant deduplication and the scope of the fault evidence. For performance findings,
check every rate/repetition, rejected outcomes, generator/observer health, resource
quotas, profiling overhead and the limits of a shared-runner experiment.

Link the completed review from the tracking issue. Automated analysis and owner
merge authorization do not substitute for this record.
