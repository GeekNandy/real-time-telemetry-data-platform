# Real-Time Telemetry Data Platform

A focused Java telemetry pipeline: authenticated HTTP intake, Kafka acknowledgement,
and replay-safe PostgreSQL persistence. The implementation makes the delivery
boundary explicit and exposes the failure cases behind it.

**Current status:** the Java 21 Maven build, executable JAR packaging and 38 core/unit tests pass locally. Kafka/PostgreSQL integration execution and the container build are still pending.

## What the API promises

- `202 Accepted` is returned only after Kafka acknowledges the append. It does
  not mean PostgreSQL has stored the event.
- Repeating the same `(tenant_id, event_id, payload)` may append multiple Kafka
  records; the database keeps one row.
- Reusing an ID with a different canonical payload preserves the first database
  row and quarantines the conflicting record. This is detected asynchronously.
- A timeout or `503` is ambiguous: the record may already exist in Kafka. Retry
  with the **same event ID and payload**.
- The consumer commits its source offset after the database transaction commits,
  or after confirmed publication to the dead-letter topic for a non-retryable
  record. This is at-least-once delivery with an idempotent sink.

The demo runs one broker. Its acknowledgement is not protection against the loss
of that broker's disk. The replicated profile requires a separately provisioned
cluster; it does not create one.

## Read the implementation

| Question | Starting point |
| --- | --- |
| When is a request accepted? | [IngestionEngine](core/src/main/java/dev/sharad/telemetry/core/IngestionEngine.java) |
| How are concurrent sends bounded? | [AdmissionController](core/src/main/java/dev/sharad/telemetry/core/AdmissionController.java) |
| How is replay made safe? | [PostgresEventSink](service/src/main/java/dev/sharad/telemetry/service/PostgresEventSink.java) |
| When is the source offset committed? | [DeliveryProcessor](core/src/main/java/dev/sharad/telemetry/core/DeliveryProcessor.java) |
| How are tenant identities trusted? | [SecurityConfiguration](service/src/main/java/dev/sharad/telemetry/service/SecurityConfiguration.java) |
| What do the real dependency tests assert? | [PlatformIT](service/src/test/java/dev/sharad/telemetry/service/PlatformIT.java) |

The core is compatible with Java 17. The full service build targets Java 21,
Spring Boot 4.1.2, Kafka 4.0.0 and PostgreSQL 17.11.

## Verify

Requires JDK 21, Maven 3.6.3+ and a running Docker daemon for integration tests:

```bash
./scripts/verify-core.sh
mvn --batch-mode --no-transfer-progress -Pintegration verify
```

The first command needs only a JDK and treats compiler warnings as errors. It uses
the same core cases as JUnit. The second compiles the complete service and runs
real Kafka/PostgreSQL tests; it must fail if Docker is unavailable. Plain
`mvn verify` runs unit tests and packages the service, but excludes `*IT`.

## Local demonstration

Requires Docker Compose, Python 3 and curl:

```bash
./scripts/dev-up.sh
docker compose logs -f service
# Once startup completes, in another terminal:
./scripts/demo-event.sh
```

The startup script creates local random credentials in an ignored `.env`.
Demo HS256 tokens are for this loopback-only setup. Deployed environments must
use the issuer mode with an external JWT issuer, TLS and broker/database
authentication. Stop with `docker compose down`; add `-v` only when deliberately
discarding the local event data.

The request body has no tenant field. Tenant identity comes from a signed JWT
with the expected issuer, audience, expiry, scope and `tenant_id` claim.

## Design and evidence

- [Architecture and delivery decisions](docs/design.md)
- [Verification matrix and remaining gates](docs/verification.md)
- [Performance methodology](docs/performance.md)
- [Operations and security boundaries](docs/operations.md)
- [Contribution workflow](CONTRIBUTING.md)

The initial `main` commit has an empty tree. All source, documentation and CI
changes are developed on `feature/platform-foundation` and proposed through a PR.
Until that PR is merged, use the feature branch to inspect or run the project.
No independent review or production deployment is claimed.
