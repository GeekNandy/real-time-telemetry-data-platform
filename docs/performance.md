# Performance and profiling protocol

No pipeline throughput, end-to-end p99 or production capacity is claimed.

## HTTP experiment

After `./scripts/dev-up.sh`, use a token file rather than a bearer token in command
arguments. Keep the output under ignored `data/` until it is reviewed:

```bash
umask 077
python3 scripts/demo-token.py --tenant experiment > data/auth/experiment.token
python3 scripts/load.py --token-file data/auth/experiment.token \
  --rate 10 --duration 10 --concurrency 16 --output data/experiments/run-01
```

The generator follows a fixed schedule independently of request completion. Its
bounded executor records client-capacity drops instead of silently reducing the
offered rate. It records scheduled, started and completed times separately,
HTTP outcomes, raw synthetic IDs, transport ambiguity and nearest-rank
percentiles. The reconciliation phase queries every 202 ID until visibility or a
settlement deadline; zero accepted requests cannot pass reconciliation. Ambiguous
transport outcomes are tracked separately and are not silently counted as losses.

Visibility is measured during a post-load reconciliation sweep. That measurement
includes sweep/polling delay and is an upper bound; it is not an instrumented
commit timestamp or an end-to-end latency SLO. HTTP elapsed time covers local
spool append, Kafka acknowledgement and delivery checkpointing. It does not cover PostgreSQL commit.

The short CI run establishes HTTP/persistence correctness. For capacity work,
warm the system separately, run at least ten minutes at each rate and use three
fresh application runs. Save the full range of results and rejections. Measure
consumer lag and resource use concurrently; a post-load visibility sweep alone
cannot establish a real-time persistence SLO. Increase the offered rate until a
stated latency, rejection, lag or resource budget fails. Record payload sizes,
tenant mix, CPU/memory quotas, volume filesystem/IOPS, dependency versions and
image digests, source revision and dirty state. Separate the generator host from
the system under test and check generator delay before interpreting saturation.

The client verifies event IDs in acceptance receipts and query bodies. A 200 with
a different identity does not pass reconciliation. Invalid and truncated responses
are tracked as ambiguous client outcomes. The --timeout option is a socket I/O
timeout; use an external experiment deadline for a hard wall-clock budget. The
Python client is a bounded reference generator, not evidence that the generator
cannot saturate.

Run `python3 scripts/test-load.py` for positive, missing-ID, wrong-ID, invalid-
receipt, truncated-response and invalid-configuration contracts. CI deployment
smokes also use this same generator and require all twenty offered events to be
accepted and visible.

## Whole-service Java profiling

The image includes `profile-safe.jfc`, based on the JDK profiling configuration
with initial environment, initial system properties and system-process events
disabled. Start an isolated collector/worker experiment with an extra JVM option:

```
-XX:StartFlightRecording=filename=/profiles/collector.jfr,settings=/app/profile-safe.jfc,dumponexit=true,maxsize=128m,maxage=5m
```

Use a Compose override for that role's `JAVA_TOOL_OPTIONS`, retaining its resource
limits. Give the worker a different filename. After the workload, stop both roles
gracefully to dump recordings and use `docker compose cp` to copy `/profiles` out.
Use `jfr summary` and JDK Mission Control to inspect CPU samples, allocation,
lock/park contention, filesystem latency and GC. Compare profiled and unprofiled
runs; sampling changes the workload. Review recordings before sharing: disabling
the listed metadata events is not a guarantee that every diagnostic is free of
sensitive information. No whole-service JFR finding is published yet.

## Reproducible CI experiment

The **Capacity experiment** workflow has a short `pilot` mode that validates the
complete harness, and a manual `full` mode. Full mode uses standard public Linux
runners with nine independent cases: 10, 50 and 150 offered events/second, each
repeated three times on fresh runtimes. Every case compares a pinned local-spool
acceptance baseline, Kafka-confirmed acceptance and a profiled Kafka-confirmed run.
Each phase starts fresh dependency/application volumes, warms at up to 10/s for
one minute, then measures ten minutes. Even repetitions reverse the phase order.

Both roles have one CPU and 768 MiB, use the three-broker RF3/minISR2 reference,
one synthetic tenant, 64 bounded client slots and 10% trace sampling. The harness
records exact source/image identities, host CPU/memory, filesystem observations,
raw request outcomes/timestamps, acknowledged-ID reconciliation, live source lag,
buffer occupancy, CPU/memory and container restarts. A persistent Java admin
observer reads committed offsets before log ends once per second, so the
non-atomic lag observation includes the interval between those reads. Docker and
HTTP snapshots run every five seconds. The generator and observers share the runner with the services.

The stated workload budgets are at least 99% offered-to-202 success, scheduled
response p99 <= 1000 ms, generator-delay p99 <= 50 ms, observed source lag <= two
seconds of offered traffic, at least 90% lag-sample coverage, application memory
below 90% of its quota, no OOM/restart and complete reconciliation of accepted IDs.
Failures are retained as measured results. All three unprofiled repetitions must
pass before an offered rate is described as a passing tested point. That does not
identify a production ceiling. A separate generator host and controlled storage
remain necessary for broader capacity claims.

Whole collector/worker recordings use the sanitized JFR configuration. Analysis
is restricted to the measured load window and reports sampled CPU stacks,
weighted allocation estimates, GC pauses and thresholded file/park events.
Profiled/unprofiled comparisons expose measurement overhead. Raw synthetic request
logs and selected diagnostic recordings are retained for one day to bound artifact
storage; aggregate results and recording/report hashes can be committed as evidence.
The pilot is explicitly ineligible for ten-minute capacity conclusions.

```bash
python3 scripts/test-capacity.py
python3 scripts/capacity-report.py --aggregate downloaded-full-run-artifacts > results.json
```

## Component overhead only

`./scripts/benchmark-core.sh 8 50000` uses eight closed-loop clients and an
immediate-completion log double. Each client warms up for 10,000 operations, then
collects 50,000 samples. It excludes HTTP, JWT verification, Avro, filesystem sync,
Kafka and PostgreSQL. The script reports revision, dirty state, JVM settings,
processor count, outcomes and lease reconciliation. `PROFILE=1` adds a component
recording with sensitive metadata categories disabled. These results cannot be
presented as durable pipeline capacity; the closed-loop workload has coordinated
omission and does not preserve a fixed offered rate during stalls.
