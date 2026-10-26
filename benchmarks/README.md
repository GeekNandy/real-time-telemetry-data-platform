# Reproducible experiments

No pipeline throughput or latency result is published yet. The harness records raw
request outcomes, generator delay, client drops and reconciliation of every HTTP202 ID.
The component microbenchmark in `core` exercises an immediate-acknowledgment in-memory
log; it does not measure disk durability, Kafka, PostgreSQL or HTTP capacity.

See [the experiment protocol](../docs/performance.md). Put new measurements under
`data/experiments/` first. Promote a report only after recording the exact clean revision,
resource limits, dependency versions, fault schedule and accepted-ID reconciliation.
Never commit tokens, `.env`, database dumps or unreviewed flight recordings.
