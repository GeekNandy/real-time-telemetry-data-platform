# Real-Time Telemetry Data Platform

Java 21 components for reliable telemetry ingestion and processing.

## Components

| Module | Responsibility |
| --- | --- |
| `core` | Immutable events, atomic tenant admission, cancellation-safe durable work and delivery ordering |
| `platform-config` | Validated resource limits and production transport policy |
| `durable-buffer` | Checksummed persistent buffering, exclusive ownership and recoverable delivery checkpoints |

## Build and verify

Requires JDK 21. The Maven wrapper downloads and checksum-verifies Maven 3.9.12.

```bash
./mvnw --batch-mode --no-transfer-progress verify
./scripts/verify-core.sh
```

The core runner and its JUnit adapter execute the same contracts; do not count them twice.

The persistent buffer synchronizes file contents and its directory before append completion. Its recovery guarantee assumes the volume survives; it is not replicated storage. See [buffer recovery](docs/buffer.md).

## Development

Changes are proposed from feature branches and merged through PRs after the relevant checks pass. The foundation was extracted from [draft PR #2](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/2); the [review sequence](docs/review-sequence.md) explains its decomposition. Owner-authorized merging is not independent human approval.
