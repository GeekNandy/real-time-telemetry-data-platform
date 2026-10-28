# Measured telemetry workload and whole-service profiles

**10 offered events/second** was the highest tested point that met every stated workload budget in all three unprofiled Kafka-confirmed repetitions. This is a result for the recorded shared CI environments; it does not identify a production capacity ceiling.

Observed on **28 October 2026** in [Capacity experiment 40638650103](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40638650103). All nine fresh cases and all 27 ten-minute measurement phases completed. The measured phases scheduled **1,134,000 synthetic events** and returned HTTP 202 for **773,472**. Every HTTP 202 identity reconciled to the downstream store. Read rejected/client outcomes alongside accepted-ID reconciliation in the [complete aggregate](../evidence/capacity-2026-10-28/results.json).

## Workload and provenance

- Measured Kafka-confirmed source: `09832b42d2cb0edb25ee11748b45c3adff5edaf4`; tree `2815b38cf9301d9a7af5e2777e021bf93d5c4653`.
- Local-spool acceptance comparison: `7df5bb9d56bb666b95c00b36bfb13af12cb02130`. This reference has a different acknowledgement contract.
- Offered rates: 10, 50 and 150 events/second; three fresh repetitions per rate. Each case compares the baseline, Kafka-confirmed acceptance and a profiled Kafka-confirmed phase; repetition 2 reverses phase order.
- Every phase uses fresh application/dependency volumes, a 60-second warmup at 10/s, then 600 measured seconds. Reconciliation can continue for up to 600 seconds after load.
- One synthetic tenant, 64 bounded generator slots, event type `performance.sample` and one attribute (`platform=synthetic-load`). Request envelopes are 192 UTF-8 bytes with fractional seconds, or 185 when the timestamp has zero microseconds; those sizes are derived from the pinned generator.
- Collector and worker each have one CPU, 768 MiB, `ActiveProcessorCount=1` and 10% trace sampling. Kafka uses three data brokers, RF3, `acks=all` and minISR2; all share one controller.
- Generator, services and observers share each standard Ubuntu runner. Host variation and local-volume IOPS are not controlled. Source lag is sampled every second; Docker/HTTP observations every five seconds.

The [provenance record](../evidence/capacity-2026-10-28/provenance.json) retains all nine environments, image IDs, quotas, filesystem observations, phase order, artifact/report/recording hashes and a raw-request audit. All 27 request streams were checked for count/outcome consistency, per-stream identity uniqueness, reconciled 202 identities and recomputed latency summaries.

## Kafka-confirmed workload results

Budgets: at least 99% of offered requests return 202; scheduled response p99 <=1000 ms; generator-delay p99 <=50 ms; maximum observed source lag <=two seconds of offered traffic; observer coverage; application memory <90% of quota; no OOM/restarts; and complete reconciliation of accepted IDs. Every unprofiled repetition must pass.

| Offered/s | Repeat | 202 / offered | HTTP p99 ms | Scheduled p99 ms | Max source lag | Budgets |
| --- | --- | --- | --- | --- | --- | --- |
| 10 | 1 | 6,000 / 6,000 | 358.061 | 358.741 | 2 | Pass |
| 10 | 2 | 6,000 / 6,000 | 83.156 | 84.545 | 1 | Pass |
| 10 | 3 | 6,000 / 6,000 | 124.648 | 125.05 | 2 | Pass |
| 50 | 1 | 29,699 / 30,000 | 1612.642 | 1612.87 | 7 | Fail |
| 50 | 2 | 29,377 / 30,000 | 1947.145 | 1947.287 | 3 | Fail |
| 50 | 3 | 29,939 / 30,000 | 1228.206 | 1229.492 | 4 | Fail |
| 150 | 1 | 57,619 / 90,000 | 1406.918 | 1407.176 | 6 | Fail |
| 150 | 2 | 47,278 / 90,000 | 2306.63 | 2310.304 | 6 | Fail |
| 150 | 3 | 40,468 / 90,000 | 3244.122 | 3244.271 | 5 | Fail |

Failed budgets are retained as observations:

| Phase | Offered/s | Repetitions | Failed budgets |
| --- | --- | --- | --- |
| `acknowledged` | 50 | 1, 2 | 202 fraction below 99%; Scheduled p99 above 1000 ms |
| `acknowledged` | 50 | 3 | Scheduled p99 above 1000 ms |
| `acknowledged` | 150 | 1, 2, 3 | 202 fraction below 99%; Scheduled p99 above 1000 ms |
| `acknowledged-profiled` | 50 | 2 | 202 fraction below 99%; Scheduled p99 above 1000 ms |
| `acknowledged-profiled` | 50 | 1, 3 | Scheduled p99 above 1000 ms |
| `acknowledged-profiled` | 150 | 1, 2, 3 | 202 fraction below 99%; Scheduled p99 above 1000 ms |
| `spool-baseline` | 150 | 1, 2, 3 | 202 fraction below 99% |

In the Kafka-confirmed runs, non-202 scheduled work was dropped by the bounded client when its 64 active slots were full; it was not sent to the API. At 150/s the baseline also returned retryable 503s. Low scheduling delay among started requests does not negate these client drops. These workloads therefore do not isolate a server throughput ceiling.

## Acceptance latency and profiling comparison

These are per-run p99 values, not pooled percentiles. Differences compare complete fresh runtimes; phase ordering, shared-host effects and differences in accepted work when the client saturates remain confounders. Negative profiling differences are not evidence that profiling improves performance.

| Offered/s | Repeat | Baseline HTTP p99 ms | Kafka-confirmed HTTP p99 ms | Profiled HTTP p99 ms | ACK p99 difference ms | Profiled p99 change % |
| --- | --- | --- | --- | --- | --- | --- |
| 10 | 1 | 170.871 | 358.061 | 406.132 | 187.19 | 13.4 |
| 10 | 2 | 10.629 | 83.156 | 118.91 | 72.527 | 43.0 |
| 10 | 3 | 11.86 | 124.648 | 115.698 | 112.788 | -7.2 |
| 50 | 1 | 17.967 | 1612.642 | 1449.265 | 1594.675 | -10.1 |
| 50 | 2 | 284.257 | 1947.145 | 1759.697 | 1662.888 | -9.6 |
| 50 | 3 | 14.569 | 1228.206 | 1234.12 | 1213.637 | 0.5 |
| 150 | 1 | 20.855 | 1406.918 | 1710.297 | 1386.063 | 21.6 |
| 150 | 2 | 78.815 | 2306.63 | 1654.884 | 2227.815 | -28.3 |
| 150 | 3 | 284.94 | 3244.122 | 3086.246 | 2959.182 | -4.9 |

## Resources and whole-service JFR

CPU is the Docker observation percentage (100% is one fully used CPU). Memory is a percentage of each role's quota. The table shows ranges across the three unprofiled repetitions at each rate.

| Offered/s | Role | CPU p95 range % | Peak memory range % |
| --- | --- | --- | --- |
| 10 | collector | 10.92–16.68 | 35.72–36.94 |
| 10 | worker | 3.57–5.66 | 37.17–38.17 |
| 50 | collector | 36.64–46.42 | 39.49–39.97 |
| 50 | worker | 15.87–18.64 | 36.92–38.07 |
| 150 | collector | 32.92–60.72 | 41.73–44.02 |
| 150 | worker | 9.99–20.62 | 37.99–38.29 |

JFR used the sanitized profile configuration and a 64 MiB/30-minute bound for both roles. Events are selected by start timestamp within the 600-second measured window; long waits can extend beyond it. Allocation weights are estimates, execution samples are not exact CPU percentages, and file/park counts include only configured thresholded events.

| Offered/s | Role | Execution samples across 3 runs | Estimated allocation MiB/run | Total GC pause ms/run | Max GC pause ms/run |
| --- | --- | --- | --- | --- | --- |
| 10 | collector | 1,346 | 965.35–971.47 | 210.17–249.63 | 89.78–103.40 |
| 10 | worker | 526 | 274.34–285.26 | 33.33–46.67 | 3.45–6.13 |
| 50 | collector | 3,013 | 4527.30–4638.69 | 621.72–714.94 | 92.31–128.27 |
| 50 | worker | 1,092 | 1085.07–1135.45 | 101.42–148.69 | 4.67–12.05 |
| 150 | collector | 4,883 | 5504.18–8827.47 | 1402.31–2221.30 | 82.16–106.29 |
| 150 | worker | 1,858 | 1320.24–2072.56 | 123.35–201.08 | 4.25–12.97 |

## Profile findings and limits

- Collector sampled allocations were higher than worker allocations at every rate. Byte arrays led the retained allocation-class weights in every recording; strings, object arrays and map structures also appeared. These are weighted sampling estimates, not exact allocation counts or evidence of a leak. Memory remained below its stated budget and no container OOM/restart was observed.
- Retained CPU leaf samples frequently included map operations, Kafka metrics and logging paths. Their small, varying counts do not identify an exclusive CPU bottleneck or justify assigning exact CPU percentages to individual methods.
- Collector GC pause totals ranged from about 0.21 to 2.22 seconds per 600-second measured window; the largest recorded single pause was 128.27 ms. The recordings do not establish GC as the cause of the 1.2–3.2-second unprofiled acceptance p99 at the higher rates.
- No thresholded file-read/write events were recorded at 10/s. The collector recorded ten reads in the 50/s second repetition (maximum 84.95 ms) and three in the 150/s third repetition (maximum 48.27 ms). Absence of these events does not establish low synchronous-disk latency: the selected event types and thresholds do not capture every filesystem operation.
- Park events include long background waits, and collector monitor-entry counts vary substantially between runs. Counts alone are not evidence of harmful lock contention; diagnosing a specific lock requires its stack, duration and request correlation.
- **Inference:** the observed failing budgets concern acceptance latency and offered-to-202 success, while unprofiled worker source lag stayed at seven records or fewer and worker CPU p95 stayed below 21% of one CPU. The next measurement should separate collector queue, filesystem synchronization/checkpoint and Kafka-ack time on controlled storage with an independent generator before selecting a batching or concurrency change. This is a diagnostic direction, not an established root cause.

## Interpretation and reproduction

HTTP elapsed time ends at Kafka acknowledgement plus the collector delivery checkpoint. It does not include PostgreSQL commit. Post-load query visibility includes the reconciliation sweep and polling delay, so it is an upper bound rather than an instrumented end-to-end persistence SLO.

Read all rates, repetitions, rejected outcomes, generator delays and lag/resource budgets together. The experiment does not establish controller-quorum/AZ availability, cloud production operations or a globally representative tenant/payload mix.

The aggregate and provenance remain in Git. Original synthetic request streams and selected collector/worker recordings use one-day artifact retention. While artifacts are available, download all nine archives from the workflow, verify their SHA-256 digests and extract into one directory:

```bash
python3 scripts/capacity-report.py --aggregate downloaded-full-run-artifacts > results.json
```

For a fresh experiment, use the [performance protocol](performance.md) and run the full workflow. Record its new revision and environment rather than treating a later run as the original observation.
