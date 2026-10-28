# Reactive runtime contracts

The HTTP service uses Spring WebFlux on Reactor Netty. Blocking work stays on
bounded dedicated schedulers: spool encoding/force on the ingestion scheduler,
JDBC on the eight-thread, bounded-queue database scheduler. Kafka publication runs
on its own bounded dispatcher window; a client disconnect cannot discard durable
accepted work. Database connect/socket/statement timeouts bound work beyond the
HTTP subscription lifetime.

`ReactiveThreadingTest` runs with the checksum-resolved BlockHound test agent at
JVM startup. Its positive canary must reject `Thread.sleep` on a Reactor parallel
thread, so a missing detector cannot make the suite pass. The other test serves
real Netty HTTP requests through the actual query controller and database
scheduler. Four deliberately blocked JDBC queries must leave `/health`
responsive; cancelling one HTTP request must interrupt that query before the
other queries are released. The remaining requests finish normally and active
query work returns to zero. No broad blocking exemptions are configured.

Run `./mvnw -pl service -am test`. The normal required Java CI check runs these
contracts along with cancellation, acceptance, spool and Kafka contracts. The
agent is test-scoped and absent from production and benchmark processes.

These checks establish executable threading and cancellation behavior. Sustained
production latency, operational history and leak-free behavior require separate
SLO/soak evidence; see [SLO profiles](slo.md) when available. A passing test is not
a claim of prior production employment or an on-call rotation.
