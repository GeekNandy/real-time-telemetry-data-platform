# Persistent buffer and recovery

Each collector volume has one process owner. Append writes a bounded checksummed
frame, forces file contents, atomically renames it and forces the directory.
An append completion assumes the volume is retained; it does not provide replication.

Delivery checkpointing and file reclamation are separate recoverable states.
After confirmed downstream delivery, the delivered checkpoint is forced before
the record is removed. If unlinking fails, a later acknowledgement retries cleanup
even though the checkpoint already covers the sequence. An already-absent delivered
file is safe to reclaim from memory. Capacity is released only after that cleanup.

Append and checkpoint have separate operation locks. Brief state updates use one
monitor; file reads and synchronization run outside it. Checkpoint reads share the
checkpoint lock so reclamation cannot remove a file during a read. One appender
assigns sequences and consumes budget, while checkpoints can only release budget.
An in-progress append becomes available to the dispatcher after its directory
sync attempt finishes. A renamed record remains charged and replayable if that
sync reports an ambiguous failure.

Shutdown waits for both disk protocols before releasing exclusive volume ownership.
Kafka acceptance does checkpoint I/O outside its waiter monitor, then settles
receipts under that monitor after append has registered its waiter. Successful
receipts still require Kafka acknowledgement and the durable local checkpoint.

Tests cover quotas, concurrent append, checksums, exclusive ownership, process
termination, restart, out-of-order acknowledgement, and a real filesystem unlink
failure after checkpointing. The latter must recover without restarting the process.
Controlled stalls around real filesystem synchronization also verify independent
append/checkpoint progress, responsive snapshots, shutdown ownership, and recovery
of an append whose directory sync failed.

```bash
./mvnw --batch-mode --no-transfer-progress verify
```

File-per-event synchronization favors a small auditable protocol. Its throughput
and filesystem overhead require measurement before any capacity claim.
