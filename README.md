# Real-Time Telemetry Data Platform

Java 21 components for reliable telemetry ingestion and processing.

## Components

| Module | Responsibility |
| --- | --- |
| `core` | Immutable events, atomic tenant admission, cancellation-safe durable work and delivery ordering |
| `platform-config` | Validated resource limits and production transport policy |
| `durable-buffer` | Checksummed persistent buffering, exclusive ownership and recoverable delivery checkpoints |
| `event-schemas` | Compatible Avro envelopes and size/deadline-bounded Schema Registry access |
| `platform-observability` | Fixed-cardinality metrics and W3C trace propagation |
| `service` | Authenticated collector and persistence runtime integration |

## Build and verify

Requires JDK 21. The Maven wrapper downloads and checksum-verifies Maven 3.9.12.

```bash
./mvnw --batch-mode --no-transfer-progress verify
./scripts/verify-core.sh
```

The core runner and its JUnit adapter execute the same contracts; do not count them twice.

The persistent buffer synchronizes file contents and its directory before append completion. Its recovery guarantee assumes the volume survives; it is not replicated storage. See [buffer recovery](docs/buffer.md).

Avro schema evolution and registry transport behavior are described in [schema contracts](docs/schemas.md).

## Collector runtime

The collector validates JWT identity and scopes, derives the tenant from the token, and returns HTTP 202 after a synchronized local-volume append. Kafka dispatch is asynchronous. Permanent collector-volume loss before Kafka acknowledgement is outside this guarantee. See [design](docs/design.md) and [operations](docs/operations.md).

```bash
./mvnw --batch-mode --no-transfer-progress -f tools/schema-registry-runtime/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath -Dmdep.outputFile=target/classpath.txt
./mvnw --batch-mode --no-transfer-progress -Pregistry-integration verify
```

The independent persistence worker commits Kafka offsets after a PostgreSQL transaction or confirmed dead-letter publication. Identical tenant/event replays are idempotent within the retention window; conflicting payloads are quarantined. [Worker operations](docs/worker.md) covers retrieval, retention and failure handling.

The complete integration suite requires Docker and runs without silently skipping missing infrastructure:

```bash
./mvnw --batch-mode --no-transfer-progress -Pintegration verify
```

## Local deployment

```bash
./scripts/dev-up.sh
./scripts/demo-event.sh
```

The Compose stack binds host ports to loopback and uses generated demo credentials. The [Helm deployment](deploy/helm/telemetry/README.md) uses independent collector and worker workloads, retained collector PVCs, non-root execution and explicit network policy. `./scripts/kind-smoke.sh` builds the image, reconciles twenty accepted IDs and restarts the collector. This is a correctness smoke, not a capacity result.

## Development

Changes are proposed from feature branches and merged through PRs after the relevant checks pass. The foundation was extracted from [draft PR #2](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/2); the [review sequence](docs/review-sequence.md) explains its decomposition. Owner-authorized merging is not independent human approval.
