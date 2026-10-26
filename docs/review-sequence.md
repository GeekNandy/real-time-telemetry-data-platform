# Foundation review sequence

The owner requested eight sequential PRs, each created from the main revision
produced by merging its predecessor. The implementation is extracted from
[PR #2](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/2),
source revision `0cec67f94cd6d7489ffd9b24429259e9ce29b7d9`.
Original history is retained; the new commits record the actual decomposition.

| Order | Scope |
| --- | --- |
| 1 | Build setup, immutable event contracts, atomic admission and shared limits |
| 2 | Persistent buffering and checkpoint/reclamation recovery |
| 3 | Compatible Avro envelopes and bounded registry transport |
| 4 | Authenticated collector, Kafka dispatch and shared observability |
| 5 | Persistence worker, idempotency, tenant retrieval and retention |
| 6 | Containers, Helm, network policy and Kubernetes smoke checks |
| 7 | AWS infrastructure reference and mocked policy tests |
| 8 | Load/reconciliation tooling, profiling methodology and evidence |

Each PR includes its relevant tests and documentation. CI grows with the available
components instead of checking out absent future modules. HTTP acceptance remains
local-volume durable in this foundation; stronger replicated acceptance is the
separate planned change in issue #3.

Owner-authorized merging is distinct from independent human review. Capacity,
whole-service profiling and real-cloud availability claims require separate evidence.
