# Persistent buffer and recovery

Each collector volume has one process owner. Append writes a bounded checksummed
frame, forces file contents, atomically renames it and forces the directory.
An append completion assumes the volume is retained; it does not provide replication.

Bounded `appendBatch` places two or more independently checksummed record frames
in one checksummed group file. One file synchronization, atomic rename and
directory synchronization cover the entire group. The whole count, framing and
physical-byte budget is validated before writing. A single record still uses its
ordinary `.evt` file and may exceed the 4 MiB group limit if allowed by the
configured record limit. Groups contain at most 256 records / 4 MiB, including
their framing. A failed temporary write exposes no member; a renamed group whose
directory sync fails remains fully charged and replayable, with ambiguous client
outcomes rather than partial success receipts.

The reader accepts existing `.evt` files and new `.grp` files in the same volume;
checkpoint format is unchanged. Group sequence range, count, lengths, checksums,
ordering and absence of overlaps are checked before admission reopens. Older
binaries cannot read group files and fail closed on them. Drain a volume fully
with this reader before rolling back to a binary that only understands `.evt`.

Delivery checkpointing and file reclamation are separate recoverable states.
After confirmed downstream delivery, the delivered checkpoint is forced before
the record is removed. If unlinking fails, a later acknowledgement retries cleanup
even though the checkpoint already covers the sequence. An already-absent delivered
file is safe to reclaim from memory. A partially delivered group retains its
entire physical-byte charge until its last pending member is checkpointed and
the shared file is removed. Recovery skips delivered members while retaining
that whole-file charge. This prevents a partially drained group from lending
still-allocated disk bytes to new admissions.

Append and checkpoint have separate operation locks. Brief state updates use one
monitor; file reads and synchronization run outside it. Checkpoint reads share the
checkpoint lock so reclamation cannot remove a file during a read. One appender
assigns sequences and consumes budget, while checkpoints can only release budget.
An in-progress append becomes available to the dispatcher after its directory
sync attempt finishes. A renamed record remains charged and replayable if that
sync reports an ambiguous failure.

Shutdown waits for both disk protocols before releasing exclusive volume ownership.
Kafka acceptance registers waiters in a short callback after directory sync and
before the appended entries become visible. Confirmation therefore needs no lock
held across another append's I/O. Successful
receipts still require Kafka acknowledgement and the durable local checkpoint.

The collector queues copied append intents on one writer, bounded by both record
count and a conservative 4 MiB byte charge, including in-progress writes. A batch
has at most 32 intents and a 1 MiB byte target. It coalesces for at most 10 ms,
or drains immediately when 32 intents are available. Expiring an HTTP waiter does
not release the queued intent's resource charge before its disk attempt finishes.
Shutdown rejects unwritten intents and waits for the in-progress writer; it never
deletes existing durable records. The API returns 202 only after downstream
publication and checkpoint confirmation, not on intent admission.

Tests cover quotas, concurrent append, checksums, exclusive ownership, process
termination, restart, out-of-order acknowledgement, and a real filesystem unlink
failure after checkpointing. The latter must recover without restarting the process.
Controlled stalls around real filesystem synchronization also verify independent
append/checkpoint progress, responsive snapshots, shutdown ownership, and recovery
of an append whose directory sync failed. Group tests also force-kill a separate
writer process, corrupt framing and checksums, mix legacy/group files, checkpoint
within a group, reject overlapping ranges, and exercise final-file unlink recovery.

```bash
./mvnw --batch-mode --no-transfer-progress verify
```

Sharing synchronization barriers is an implementation property; its effect on
whole-service latency and capacity requires measurement against the fixed SLOs.
