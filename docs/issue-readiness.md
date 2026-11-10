# Open issue readiness

This checklist was reviewed on 2026-11-10 against main
`e62ca4c9e8ff108a49d5036d482061c55a1058cb` and the subsequent operational
correctness fixes. Historical test results retain their original source and
environment; they are not fresh workload measurements.

| Requirement | Evidence and current status | Remaining gate |
| --- | --- | --- |
| [#1: platform foundation](https://github.com/GeekNandy/real-time-telemetry-data-platform/issues/1) | The issue records completed correctness, deployment and measured-workload verification. [Verification](verification.md) and [performance findings](performance-results.md) retain the original scope. | Independent human review, recording reviewer identity, reviewed source/tree, date and verdict using the [review guide](review-guide.md). Automated analysis cannot satisfy this gate. |
| [#22: fixed latency and acceptance](https://github.com/GeekNandy/real-time-telemetry-data-platform/issues/22) | The recorded 100/s pilot reconciled all 3,001 offers in each tuned phase, but scheduled p99 was 744/643 ms against the fixed 250 ms budget. The 500/s pilot also missed gates. | Diagnose with valid resource and filesystem observations, then pass the unchanged 100/s reference gates before claiming 500/s readiness. |
| #22: repeated release experiment | [The protocol](slo.md) requires eight cases and twenty phases, including repetitions, retained heap and burst recovery. Successful pilot workflows remain ineligible for release claims. | Every required full experiment and tuned gate must pass on one recorded source revision. |
| #22: reactive, security and deployment contracts | The issue records passing reactive/HTTPS identity, replicated faults, network policy, rollout/rollback/scale and restore checks on its named source. Current targeted regressions strengthen operational failure handling and evidence validation. | Run the relevant CI integration gates on the proposed fix revision; preserve the distinction between local contracts and deployed operation. |
| #22: current measurements and requirement mapping | This checklist maps the remaining requirements. No fresh capacity measurement is claimed by the operational fixes. | Publish raw audits, reconciliation, resource/JFR observations and the complete release decision from a suitable runner. |

## Measurement environment limitation

The managed local Docker 29.9.2 daemon reports `CgroupDriver=none` with
`MemoryLimit`, `CpuCfsPeriod`, `CpuCfsQuota` and `CPUSet` all false. Its rootless
configuration cannot enforce the per-container resource limits required by the
fixed profiles. A host-level CPU or memory limit does not establish separate
collector, worker and generator limits.

The SLO harness now checks these capabilities before image builds or workload
execution and retains `artifact/resource-controls.json`. Run the experiment on
a daemon with working CPU, memory and CPU-set controls. Keep the fixed profile
budgets, failed outcomes and accepted-ID reconciliation intact. Neither changing
the profile nor running an unbounded local load resolves this evidence gap.

## Operational scan scope

The review covers runtime acceptance/replay and poison handling, authenticated
Python clients, MCP health reporting, recorded-run identity reconciliation and
SLO release validation. Regression tests accompany confirmed defects. This is
automated code review and local verification, not independent human approval,
an exhaustive security audit or production operational evidence.

| Confirmed defect | Correction and regression |
| --- | --- |
| Broker-sized poison records exceeded the 32 KiB ingestion producer's dead-letter limit, blocking the valid partition suffix. | A separate bounded DLT producer and explicit topic limits preserve the full payload before offset advancement. `DeadLetterBoundsIT` exercises a real Kafka record whose quarantine headers exceed the source topic ceiling. See the [upgrade requirement](worker.md). |
| Spring's default producer failure listener logged record keys, which may identify tenants or events. | Both ingestion and DLT templates use fixed failure diagnostics without record contents or upstream exception text. Real rejected sends exercise the Boot-configured template and the DLT copy. |
| Authenticated Python requests forwarded credentials when following redirects. | Shared transport refuses redirects; actual HTTP peers verify that the second origin receives no request. Schema migration also bounds and validates returned data and stops when its compatibility-policy write is unconfirmed. |
| A release report with only an arbitrary passing gate could satisfy the aggregate validator. | The release validator requires each profile's complete gate inventory and boolean decisions. Missing latency, reconciliation, profiling, heap or recovery gates fail validation. |
| The MCP operator replaced valid degraded health responses with `UNAVAILABLE`. | HTTP 503 retains only `DOWN` or `OUT_OF_SERVICE`; status details stay private and metrics still require HTTP 200. Actual packaged HTTP/TLS tests cover both paths. |
| Recorded-run validation accepted UUID duplicates across letter casing and incorrect unresolved-ID lists. | Canonical identity comparisons require the exact unresolved accepted-ID set and at least one accepted event for successful reconciliation. Node and Chromium contracts verify the explorer. |

Python script socket timeouts remain inactivity bounds, rather than absolute
whole-response deadlines. Likewise, the documented compressed-record expansion
limit and the scope of the single-host controller exercise remain operational
limitations; the fixes do not establish broader availability guarantees.

Real cloud deployment, on-call history and multi-failure-domain availability
remain separate evidence. No paid resources are needed for the local fixes.
