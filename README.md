# Real-Time Telemetry Data Platform

A Java 21 reference platform for authenticated telemetry ingestion, persistent buffering, Kafka delivery, schema evolution and idempotent PostgreSQL persistence.

## Implementation under review

This branch intentionally contains only this README while the platform foundation is reviewed.

- [Implementation: feature/platform-foundation](https://github.com/GeekNandy/real-time-telemetry-data-platform/tree/feature/platform-foundation)
- [Pull requests](https://github.com/GeekNandy/real-time-telemetry-data-platform/pulls)
- [Scope and acceptance gates](https://github.com/GeekNandy/real-time-telemetry-data-platform/issues/1)

The implementation branch contains the source, build instructions, design and failure model, deployment configuration, runbooks and verification evidence. Refer to its verification record for observed results and outstanding checks.

HTTP acceptance follows synchronization to the collector's persistent volume. Kafka and database delivery are asynchronous; retaining that volume until confirmed Kafka publication is part of the durability assumption.

This is a reference project under verification. No production deployment or pipeline capacity is claimed.
