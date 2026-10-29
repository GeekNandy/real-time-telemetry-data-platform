# Design and fault model

## Boundaries

The collector first appends to its durable spool. The dispatcher publishes in
volume order, waits for Kafka acknowledgement, and persists a delivered checkpoint
before completing the HTTP 202 response with the actual Kafka topic/partition/offset.
The replicated reference requires three broker replicas, `acks=all`, producer
idempotence, minISR 2 and disabled unclean leader election. Startup verifies the
effective producer settings and every topic partition/configuration before HTTP
starts. The single-broker development configuration has the same acknowledgement
boundary but does not provide replicated durability.
The worker commits a source offset after a PostgreSQL transaction, or after
confirmed dead-letter publication for a non-retryable record.

These boundaries are independently recoverable. No transaction spans the
collector filesystem, Kafka and PostgreSQL.

## Durable buffer

Each private volume has one process owner enforced by a file lock. An append
writes a bounded frame to a temporary file, synchronizes the file, atomically
renames it and synchronizes the directory before waiting for publication.
The collector assembles bounded append batches: every frame is individually forced
and renamed, then one directory sync covers the batch. A failed partial batch
keeps its renamed prefix charged and replayable while every affected HTTP outcome
remains unconfirmed. Existing single-record files and checkpoint recovery remain
compatible.

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

## Bounded publication and checkpoint batching

The dispatcher reads a prefix bounded by both record count and encoded bytes
(default 64 records and 1 MiB; hard limits 256 and 4 MiB). A head larger than the
requested byte target is processed alone within the configured per-record limit.
It sends that complete window in volume order before waiting for producer
acknowledgements. Kafka idempotence and at most five in-flight requests preserve
per-partition producer order; downstream identity/fingerprint checks handle replay.
A two-millisecond coalescing interval bounds assembly delay. A notified dispatcher
waits efficiently while the spool is empty.

Only after **every** record in the prefix has a successful producer receipt does
one synchronized delivered checkpoint advance through that prefix. Every waiting
HTTP request keeps its own actual topic/partition/offset. If a send, acknowledgement
or checkpoint fails, no waiter in the unconfirmed batch succeeds; the prefix can
be republished and deduplicated. A durable checkpoint and reclamation remain
separate recoverable states. The on-disk frame/checkpoint format is unchanged.

`telemetry.dispatch.batch-records`, `telemetry.dispatch.batch-bytes` and
`telemetry.dispatch.publication-deadline-ms` configure the bounded window and its
shared publication deadline (default ten seconds). This deadline includes sends
and waits; Kafka's own bounded metadata/buffer allocation and filesystem calls can
overrun it. It does not extend the separate HTTP acknowledgement deadline.

Bounded-label histograms distinguish append encoding, intent enqueue time,
spool batch reads, batch publication and batch checkpoint time. These are operation
measurements; overlapping batch and per-request intervals are not additive stage
latencies. Batch-size observations and source/load identities accompany workload
experiments. No throughput improvement is inferred merely from the implementation.
`telemetry.spool.append.duration` includes intent queue time and its complete file/
directory synchronization attempt; `telemetry.spool.append.batch.size` records
the physical append batch. Pending intent count and byte gauges include writes
currently performing I/O and remain charged after HTTP timeout until that attempt
finishes. Warmup drain requires both the intent queue and durable backlog empty.

## Explicit failure assumptions

| Failure | Intended behavior |
| --- | --- |
| HTTP response is lost or publication exceeds the response deadline | Outcome is ambiguous; retry the same tenant/ID/content, and downstream storage deduplicates |
| Collector process is killed | Retained volume is scanned and pending entries replay |
| Kafka is unavailable or ISR is insufficient | No HTTP 202; return retryable 503 at the acknowledgement deadline, retain spool records and retry publication within bounded budgets |
| PostgreSQL is unavailable | Kafka offsets remain uncommitted; the worker retries |
| Worker commits the database and dies before offset commit | Redelivery finds the same fingerprint and produces no second row |
| DLT publication fails | Recovery must fail and the source offset remain uncommitted |
| Committed spool/checkpoint is corrupted | Fail closed and retain the data for investigation |
| Collector volume is lost before Kafka acknowledgement | Unconfirmed work may be lost; it has not received HTTP 202 |
| Collector volume is lost after HTTP 202 | The accepted record is already in Kafka under the verified acknowledgement/replication policy |
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
are retained. The bounded ingestion executor encodes and admits intents; one bounded
spool writer isolates filesystem work from the event loop. Work has a start deadline
and global/per-tenant admission budgets. A separate acknowledgement deadline defaults to
five seconds from spool append start. Timeout settles only the bounded response
waiter; it never deletes durable work. Admission stays occupied until confirmation
or that deadline. Cancellation of an HTTP subscriber cannot cancel the underlying
append/publication or release its permit prematurely. Filesystem stalls can delay
the append itself; the protocol does not turn an uninterruptible fsync into a hard
wall-clock bound.

Append registration runs after directory synchronization and before the new entries
become visible to the dispatcher. Fast publication cannot overtake registration,
and an already-confirmed receipt need not wait for another append's disk I/O.
Admissions, registration, timeouts, close and settlement use a short state monitor;
no disk I/O or subscriber callback holds it. One removable timer per waiter is
bounded by global admission capacity; pending intent count and bytes have separate
bounds so timeout cannot open an unbounded disk queue. Subscriber callbacks run
outside that monitor and the filesystem protocol. Recovery needs
no HTTP waiter; the spool is independently bounded by its encoded byte and record
budgets, and one ordered dispatcher retries its unconfirmed prefix with capped backoff.

Immutable records and copied maps cross the executor boundary. Monitor release/
acquisition, executor queue publication, atomic settlement and future completion
provide the required visibility. Completion callbacks run outside the admission
monitor. Counter snapshots are operational observations, not an atomic audit ledger.

The per-tenant limit bounds requests awaiting acknowledgement; it is not a requests-per-second
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
