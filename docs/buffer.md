# Persistent buffer and recovery

Each collector volume has one process owner. Append writes a bounded checksummed
frame, forces file contents, atomically renames it and forces the directory.
An append completion assumes the volume is retained; it does not provide replication.

Delivery checkpointing and file reclamation are separate recoverable states.
After confirmed downstream delivery, the delivered checkpoint is forced before
the record is removed. If unlinking fails, a later acknowledgement retries cleanup
even though the checkpoint already covers the sequence. An already-absent delivered
file is safe to reclaim from memory. Capacity is released only after that cleanup.

Tests cover quotas, concurrent append, checksums, exclusive ownership, process
termination, restart, out-of-order acknowledgement, and a real filesystem unlink
failure after checkpointing. The latter must recover without restarting the process.

```bash
./mvnw --batch-mode --no-transfer-progress verify
```

File-per-event synchronization favors a small auditable protocol. Its throughput
and filesystem overhead require measurement before any capacity claim.
