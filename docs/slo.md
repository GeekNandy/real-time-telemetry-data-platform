# Workload SLO and release gates

These are versioned engineering objectives for an explicit synthetic workload.
A customer SLA, production availability objective and contractual remedies require
a service owner, real traffic and an agreed measurement window. A short CI run
cannot establish a monthly availability percentage.

The authoritative profile values are [profiles.json](../tools/slo/profiles.json).
Do not relax a failing profile to make a release pass. A changed target needs its
own rationale and review; retain the original results and revision.

| Profile | Offered traffic | Duration | Collector / worker resources | Purpose |
| --- | --- | --- | --- | --- |
| Reference | 100 events/s | 10 minutes, three fresh repetitions | Each 1 CPU / 768 MiB | Required sustained workload |
| Scale | 500 events/s | 10 minutes, three fresh repetitions | Each 2 CPU / 1536 MiB | Larger tested point |
| Soak | 500 events/s | 30 minutes | Same as scale | Stability and retained-heap regression guard |
| Burst | 500 → 1000 → 500 events/s | Fixed 94-second ramp/hold sequence | Same as scale | Bounded overload and recovery |

All profiles use eight synthetic tenants, schema-valid 512-byte JSON envelopes,
three Kafka data brokers with RF3/minISR2 and `acks=all`, stable event identities,
10% trace sampling and a separate warmup. String padding controls the wire size;
it is not a claim about NVIDIA or any other production payload distribution.

The healthy sustained profiles require at least 99.9% valid 202 receipts out of
**all observed scheduled** iterations, acceptance p99 <=250 ms and p99.9 <=750 ms, live
query-visibility p99 <=2 seconds and at least 99% of accepted events visible within
that deadline. Every 202 identity must reconcile after load, with matching tenant
and event content. There may be no malformed receipts, wrong identities/content,
missing audit records, generator drops, container OOM or restarts. Generator delay
p99 must be <=25 ms. Source lag must stay within two seconds of offered traffic;
application memory must stay below 80% of quota and CPU p95 below 90% of its quota.
Observers must cover at least 90% of the measured window. The arrival timer may include or exclude one terminal boundary iteration; report the nominal and observed offered counts and require their difference to be at most one. The success denominator includes observed starts plus drops, while raw records must exactly match started/completed audit counters. Larger cardinality differences invalidate the experiment.

Each accepted request is queried during load. Its visibility observation includes
polling and network time and is an upper bound, not a database commit timestamp.
Polling backs off from 20 ms to a 200 ms cap within the same absolute deadline, bounding query amplification during lag. A separate post-load reconciliation verifies every observed 202 and reports
non-202/transport outcomes that become visible later. Timeouts and 503 outcomes
remain ambiguous; they are not silently counted as lost accepted data.

The burst gate allows explicit 429/503 backpressure, bounded client drops and
p99 completed-response time <=8 seconds; it never allows a false 202, wrong
identity/content, lost accepted ID, OOM or unbounded backlog. Accepted work must
be visible and backlog recovered within thirty seconds after offered traffic
returns to the healthy level. Report offered, attempted, rejected, dropped and
acknowledged traffic separately. A client drop is not a server rejection.

Soak adds an after-GC retained-heap growth guard over its final ten minutes. This
is a regression signal, not proof that every possible memory leak is absent.
Whole collector/worker JFR profiles, batch/stage histograms, live lag, process CPU,
working memory, cgroup limits and source/image identities accompany results.

The load generator uses a pinned, checksummed k6 binary with connection reuse,
constant arrival scheduling and bounded preallocated virtual users. It runs in a
separate CPU/memory cgroup; where four CPUs are available its CPU is separated from
the service CPU set. Services, dependencies and generator still share a machine
and storage. These constraints prevent interpreting a passing point or overload
as a production capacity ceiling. No paid cloud resources are created.

Compare the tuned source with the pinned Kafka-confirmed pre-tuning source
`41d7728d47b923e54a2cb8ef8e3eaa7e34b001cf` under identical profiles. This preserves
the acknowledgement contract in the comparison. Profiled/unprofiled differences
also retain host and workload variation; they are not isolated causal estimates.
A release claim requires every fresh repetition to pass. Short pilots validate
the harness and are ineligible for sustained/soak claims.

The protocol follows [SLO/error-budget reasoning](https://sre.google/workbook/implementing-slos/)
and k6's [arrival-rate scheduling](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/)
and [dropped-iteration accounting](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/dropped-iterations/).
