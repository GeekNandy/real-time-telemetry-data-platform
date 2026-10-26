# Performance methodology

No Kafka/PostgreSQL throughput or end-to-end latency result is currently claimed.

## Component overhead experiment

```bash
./scripts/benchmark-core.sh 8 50000 > core/target/core-overhead.json
PROFILE=1 ./scripts/benchmark-core.sh 8 50000 > core/target/core-overhead-profiled.json
```

The harness measures admission, executor dispatch and future completion using
an immediately completed broker double. It reuses a prebuilt event and does
not serialize, fingerprint, authenticate, use a network or persist data.

Each of eight closed-loop clients has one outstanding operation. Each client
warms up for 10,000 operations before collecting 50,000 samples. Percentiles use
nearest rank over the combined samples. The fixed send pool has four workers.
The harness reconciles every completion and rejects runs with a failure,
rejection or outstanding lease.

The output records the source commit, dirty state, JVM arguments, platform,
processor count, workload and exclusions. Record container CPU/memory quotas,
host contention and multiple fresh JVM forks alongside results. JFR profiling
changes the workload and its results must be reported separately.
The profiling configuration disables initial environment variables, initial
system properties and system-process metadata in recordings intended for sharing.

A short closed-loop component run cannot justify a capacity estimate or a
production latency SLO. It has coordinated omission: slow completions reduce
the request arrival rate. Use it to investigate local overhead, not to advertise
hundreds of thousands of persisted events per second.

## End-to-end experiment required before publishing capacity

Use an external, open-loop generator with a fixed arrival schedule and bounded
client concurrency. Record both scheduled and actual start times to detect
generator saturation. Preserve event IDs for all acknowledged requests and
retain ambiguous outcomes separately.

For each arrival rate, include warmup, at least ten minutes of measurement and
a drain/reconciliation phase. Increase offered load until rejection, latency,
lag or resource limits violate the proposed budget. Run at least three fresh
forks and report the range, rather than selecting the best run.

Publish:

- HTTP acknowledgement p50/p95/p99, latency from scheduled send, and status counts.
- Time from scheduled send to PostgreSQL visibility, plus source consumer lag.
- Exact accepted-ID versus stored-ID reconciliation, duplicates and quarantined IDs.
- Broker ISR/leader state, producer errors, queue occupancy and database pool wait.
- JVM CPU, heap, GC pauses and an exported JFR profile.
- Source revision, image digests, CPU/memory limits, disks, payload size,
  tenant distribution, concurrency, warmup and experiment duration.

Separate the one-broker developer setup from a replicated cluster experiment.
An acceptance SLI counts confirmed broker responses; a persistence SLI measures
eventual storage. Set numerical SLOs only after a workload and fault model are
agreed and measured.
