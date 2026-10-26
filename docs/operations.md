# Operations and security boundaries

## Identity and transport

The service verifies JWT signatures with a configured issuer in normal mode.
It checks issuer, audience, expiry and the tenant claim. Write, read and metrics
endpoints require their respective scopes. Health has no response details and
is public. The request body cannot choose another tenant.

The demo's shared HS256 key is generated locally and grants no production
trust. Broker and PostgreSQL traffic in Compose is plaintext on the local
Docker network; host ports bind to loopback. TLS, external identity integration,
broker ACLs, database roles, secrets rotation and network isolation are deployment
requirements, not features established by this prototype.

The SQL uses tenant-qualified prepared statements. Tenant separation relies
on application predicates; database row-level security is not configured.
Attribute keys are allowlisted, but their free-text values can still contain
sensitive data. An allowlist is not a PII detection system.

## Capacity and diagnosis

Defaults: 256 admitted operations per process, 64 per tenant, four send workers,
16 MiB producer buffer, 16 KiB request decode limit and eight database connections.
These defaults are starting points, not measured production sizing.

Fixed-cardinality metrics:

| Metric | Meaning |
| --- | --- |
| `telemetry_ingestion_inflight` | Admitted operations awaiting broker completion |
| `telemetry_ingestion_active_tenants` | Tenants holding admission leases |
| `telemetry_ingestion_accepted_total` | Confirmed broker completions |
| `telemetry_ingestion_failed_total` | Unconfirmed broker operations |
| `telemetry_ingestion_rejected_total` | Capacity/drain admission rejections |

Metric names are emitted through Micrometer's Prometheus registry; verify the
actual exposition during service validation. No tenant IDs or event IDs are
used as metric labels.

On elevated `429`, inspect occupancy, producer latency/errors and tenant
distribution before raising limits. On `503`, clients should reuse IDs and
payloads, because some sends may have reached Kafka despite a lost response.
During database failure, source offsets stop advancing; inspect database
connectivity, consumer lag and remaining retention before resuming traffic.

The health endpoint is not a complete readiness test for every dependency.
Queue-age timers, end-to-end visibility latency, lag collection and alerts are
follow-up instrumentation.

## Dead-letter handling

Do not delete or bulk-replay records without examining why they were quarantined.
For poison, fix the producer/schema contract before replay. For ID collision,
identify the authoritative payload; replaying the conflicting payload unchanged
cannot overwrite the original row.

Keep access to dead-letter records at least as restricted as source telemetry.
Retention, audit policy and a reviewed replay tool are not implemented.

## Shutdown and recovery

Stop intake through the service's graceful shutdown, allow admitted sends to
settle, and allow consumer transactions to complete. An expired drain deadline
does not imply data loss or successful acceptance; the outcome is unconfirmed.

After restart, Kafka may redeliver a database-committed event whose offset was
not committed. The primary key and payload fingerprint prevent a second row.
Service-process termination, rebalance behavior and replicated broker failure
still require the explicit verification listed in the matrix.

## Local data

`docker compose down` stops the demo and retains volumes. `docker compose
down -v` discards the source log and database. Never use the latter as a recovery
step for a dataset you need to retain.
