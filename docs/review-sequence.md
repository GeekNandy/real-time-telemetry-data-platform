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

## Completed predecessors of the final tooling PR

| Step | Merged PR | Tested source | Passing run |
| --- | --- | --- | --- |
| 1 | [#4](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/4) | `c9348ecbb21c` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40345796709) |
| 2 | [#5](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/5) | `d07a14ed83f4` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40347098253) |
| 3 | [#6](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/6) | `b4c0b5095e77` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40348037167) |
| 4 | [#7](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/7) | `15a77ee6803c` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40349646799) |
| 5 | [#8](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/8) | `4e74a38369ec` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40351688418) |
| 6 | [#9](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/9) | `70df3502533e` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40358095968) |
| 7 | [#10](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/10) | `671a6e86b544` | [CI](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40359885256) |

The final PR and merge are available in the repository history.
