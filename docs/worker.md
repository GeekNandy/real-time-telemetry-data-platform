# Persistence worker

Run with SPRING_PROFILES_ACTIVE=worker to consume Kafka independently of HTTP
collection. Configure DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD, the broker,
registry and JWT issuer for the actual environment. The worker query port defaults
to 8082; collector defaults to 8080. With neither role active, both run in one process.

The database transaction inserts a tenant-qualified event identity and a canonical
fingerprint. An identical replay is a duplicate. Different payloads under the same
identity are quarantined. Successful transaction completion precedes Kafka offset
acknowledgement; an interrupted commit/ack gap may replay safely.

Poison envelopes and collisions use confirmed dead-letter publication before the
source offset advances. Registry, broker and database outages remain retryable.
Do not skip offsets or weaken broker policy to clear lag. Restore the dependency,
require lag to drain and reconcile accepted IDs.

The source topic declares `max.message.bytes=1048588` (Kafka's default batch
ceiling). The DLT declares `max.message.bytes=2097152` and uses a separate,
bounded producer with a 2 MiB request limit. This leaves room for original keys,
headers and the added quarantine metadata when a malformed record approaches
the source ceiling. The collector's producer request limit remains 32 KiB.
The DLT producer inherits broker authentication, `acks=all`, idempotence and the
bounded producer buffer, and closes with the worker.
Both producer templates use a fixed send-failure diagnostic without copying
record keys, payloads or upstream exception text into the producer listener log.

Startup verifies the effective source ceiling is positive and no greater than
1,048,588 bytes, and the DLT ceiling is at least 2 MiB. Existing DLT topics need
the following configuration update before upgrading; topic declarations do not
silently rewrite existing broker configuration. Use the deployment's authenticated
Kafka CLI settings and configured topic names:

```bash
kafka-configs.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --command-config "$KAFKA_CLIENT_PROPERTIES" \
  --entity-type topics --entity-name telemetry.events.v1.dlt \
  --alter --add-config max.message.bytes=2097152
```

External producers must also keep each serialized, uncompressed source record,
including its key and headers, within the source ceiling. Kafka enforces topic
limits on compressed batches, so a heavily compressed external record can expand
beyond the supported quarantine budget. Such a publication failure keeps the
source offset uncommitted for operator remediation; payloads are never truncated
or discarded to advance the partition. The real-Kafka `DeadLetterBoundsIT`
regression sends a near-limit poison payload whose DLT metadata crosses the source
ceiling, verifies the complete quarantined payload, and reconciles the valid
suffix and committed offset.

GET /v1/events/{id} requires telemetry.read and derives its tenant from the JWT.
JDBC queries use a bounded scheduler instead of the HTTP event loop. A transient
404 is expected before asynchronous persistence finishes. Query storage failures
return 503 with Retry-After.

Rows are removed in bounded sweeps after thirty days by default, measured from
storage time. Deduplication is bounded by retention of identity/fingerprint rows.
Deleting a database row does not erase Kafka, DLT, spool files or backups.

The integration profile requires Docker and verifies PostgreSQL failure paths,
plus real collector/broker/registry recovery. It does not silently skip missing Docker.

```bash
./mvnw --batch-mode --no-transfer-progress -f tools/schema-registry-runtime/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath -Dmdep.outputFile=target/classpath.txt
./mvnw --batch-mode --no-transfer-progress -Pintegration verify
```

## Bounded poll checkpoints

The listener processes each poll sequentially, with fifty records by default and
a startup-validated hard cap of 256. It commits offsets synchronously once after
all transactions in that poll complete. A failure identifies its exact index:
only already-stored preceding records may advance. Retryable database/registry
failures preserve the failed record and suffix. Poison data and collisions still
require confirmed DLT publication before the recovered offset advances. A commit
failure replays the already-stored batch through tenant/event deduplication.
No parallel record processing is allowed inside this indexed-failure contract.

`telemetry.persistence.batch.size` and `telemetry.persistence.batch.commit` expose
bounded batch size and offset-commit timing without event or tenant labels. Unit
contracts check partial failures, poison, replay and bounds; the real Kafka/DB
integration injects a poison record between durable prefix and valid suffix rows
in one multi-record poll and reconciles recovery.

Container profiles use G1 with explicit initial/maximum heap percentages and a
soft pause target. Workload SLOs remain measured outcomes; JVM targets alone do
not establish latency. See [the fixed SLO protocol](slo.md).
