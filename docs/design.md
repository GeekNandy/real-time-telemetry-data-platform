# Design and fault model

## Boundaries

The collector accepts an event after durable local append. The dispatcher
publishes in volume order, waits for Kafka's configured acknowledgement policy,
and only then persists a delivered checkpoint and reclaims the spool entry.
The worker commits a source offset after a PostgreSQL transaction, or after
confirmed dead-letter publication for a non-retryable record.

These boundaries are independently recoverable. No transaction spans the
collector filesystem, Kafka and PostgreSQL.

## Durable buffer

Each private volume has one process owner enforced by a file lock. An append
writes a bounded frame to a temporary file, synchronizes the file, atomically
renames it and synchronizes the directory before returning acceptance.

Frames and delivered checkpoints are checksummed. Recovery validates committed
entries, removes incomplete temporary files, and fails closed on corrupted
acknowledged data. It never silently discards a corrupt committed record.

A delivered checkpoint advances only in order and is synchronized before record
reclamation. A crash between Kafka acknowledgement and checkpointing can replay
the record. A crash after checkpointing but before reclamation can leave a stale
file, which recovery safely removes.

The file-per-record design favors a small, auditable durability protocol over
maximum write throughput. Fsync, directory updates and per-record files are
known costs that must be measured. Pending record count and total encoded bytes
are bounded. File metadata is cached so readiness and metric reads perform no
disk I/O. The byte budget counts encoded records, not filesystem blocks;
provision volume headroom for metadata, temporary files and filesystem reserves.

## Explicit failure assumptions

| Failure | Intended behavior |
| --- | --- |
| HTTP response is lost after durable append | Client retries the same ID; downstream storage deduplicates |
| Collector process is killed | Retained volume is scanned and pending entries replay |
| Kafka is unavailable | Collector continues until its disk budget is exhausted, then rejects new events |
| PostgreSQL is unavailable | Kafka offsets remain uncommitted; the worker retries |
| Worker commits the database and dies before offset commit | Redelivery finds the same fingerprint and produces no second row |
| DLT publication fails | Recovery must fail and the source offset remain uncommitted |
| Committed spool/checkpoint is corrupted | Fail closed and retain the data for investigation |
| Collector volume is permanently lost before Kafka acknowledgement | Accepted data can be lost; this implementation has no collector-volume replication |
| Outage exceeds Kafka retention | Unprocessed records can expire; capacity and retention need an operational budget |

An encrypted, persistent EBS-backed PVC can protect against ordinary pod/node
replacement within its storage fault domain. It does not establish multi-region
or availability-zone fault tolerance. That is not claimed by the demo.

## Ordering and idempotency

The key is `tenant_id:event_id`. Repeats of one event share a Kafka partition.
There is no total ordering across a tenant or a session.

PostgreSQL uses `(tenant_id,event_id)` as its primary key. In one transaction,
`INSERT ... ON CONFLICT DO NOTHING` either inserts or compares a canonical
SHA-256 fingerprint. The first payload wins; a different payload for that ID
is quarantined asynchronously. HTTP acceptance does not promise an HTTP 409
for a collision detected later.

The fingerprint includes sorted attributes and a length-prefixed encoding.
It preserves timestamp precision even though PostgreSQL stores microseconds.
Deduplication is bounded by retention of the stored identity/fingerprint.

## Admission and the Java Memory Model

One monitor atomically updates global and tenant occupancy. Only active tenants
are retained. A bounded executor isolates filesystem work from the event loop;
queued work has a start deadline. Cancellation of the HTTP subscriber cannot
cancel a real append or release its permit prematurely.

Immutable records and copied maps cross the executor boundary. Monitor release/
acquisition, executor queue publication, atomic settlement and future completion
provide the required visibility. Completion callbacks run outside the admission
monitor. Counter snapshots are operational observations, not an atomic audit ledger.

The per-tenant limit bounds simultaneous appends; it is not a requests-per-second
limiter or a reservation of persistent backlog space. Gateway rate limits and
tenant backlog quotas are separate controls.

## Schema lifecycle and shared libraries

Avro v2 adds an optional collector-source field with a default, preserving both
reader directions. Production clients look up bundled schemas; registration
belongs to a controlled deployment step. The registry cache is bounded and
writers preload their IDs. A cold restart depends on the registry being
available. A registry outage must be treated as retryable infrastructure failure,
not as evidence that a source record should be quarantined.

Shared configuration validates resource relationships and production transport
policy. Shared observability propagates W3C trace context across HTTP, disk,
Kafka and the worker. Baggage, authorization headers, payloads and tenant/event
metric labels are excluded.

Registry response deadlines and checkpoint cleanup retry behavior are covered in
[schema contracts](schemas.md) and [buffer recovery](buffer.md).
