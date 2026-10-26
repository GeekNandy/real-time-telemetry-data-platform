# Runtime operations

JDK 21 runs the service JAR and its adjacent lib directory. Use an explicit
SPRING_PROFILES_ACTIVE=collector or worker for independent scaling. With neither
role selected, the application combines both roles. Configure external issuer,
broker, registry, volume and database settings for the actual environment.

HTTP acceptance follows a synchronized append to the collector's persistent
volume; Kafka and database delivery are asynchronous. Monitor buffer bytes/count,
oldest age, dispatch failures, consumer lag, database pool waits and persistence
failures. Reconcile accepted IDs after recovery; process readiness alone is insufficient.

Transient admission exhaustion returns 429. Full buffer, unconfirmed append,
draining or unavailable query storage returns retryable 503. Preserve the same
event ID and payload after an ambiguous outcome. Never clear an alert by deleting
pending files, skipping offsets or lowering Kafka's durability policy.

JWT verification requires issuer, audience, expiry, tenant_id and an appropriate
telemetry.write, telemetry.read or telemetry.observe scope. Protect the internal
HTTP port with trusted TLS termination and network controls. Production policy
requires external authentication, HTTPS registry/issuer, Kafka TLS and PostgreSQL
sslmode=verify-full. Schema registration uses a separate deployment identity.

SIGTERM stops admission, waits for real appends within its deadline and preserves
pending spool entries for replay. Preserve corrupt volumes for investigation.
Do not infer volume replication or availability-zone survival from process restart tests.

See [design](design.md), [buffer recovery](buffer.md), [schema contracts](schemas.md)
and [worker operations](worker.md). The integration profile uses real ephemeral
dependencies and records observed outcomes through the PR's CI reports.
