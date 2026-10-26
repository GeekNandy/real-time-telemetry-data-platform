# Real-Time Telemetry Data Platform

Java 21 components for reliable telemetry ingestion and processing.

## Components

| Module | Responsibility |
| --- | --- |
| `core` | Immutable events, atomic tenant admission, cancellation-safe durable work and delivery ordering |

## Build and verify

Requires JDK 21. The Maven wrapper downloads and checksum-verifies Maven 3.9.12.

```bash
./mvnw --batch-mode --no-transfer-progress verify
./scripts/verify-core.sh
```

The core runner and its JUnit adapter execute the same contracts; do not count them twice.

## Development

Changes are proposed from feature branches and merged through PRs after the relevant checks pass. The foundation was extracted from [draft PR #2](https://github.com/GeekNandy/real-time-telemetry-data-platform/pull/2); the [review sequence](docs/review-sequence.md) explains its decomposition. Owner-authorized merging is not independent human approval.
