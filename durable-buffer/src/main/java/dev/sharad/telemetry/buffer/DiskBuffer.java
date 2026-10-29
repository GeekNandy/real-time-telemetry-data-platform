package dev.sharad.telemetry.buffer;

import dev.sharad.telemetry.config.PlatformSettings;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/**
 * A bounded checksummed spool for a persistent local volume. Append completes after file fsync,
 * atomic rename and directory fsync. The single dispatcher checkpoints only after a confirmed Kafka
 * acknowledgement. Survives process restart; this is not replication or protection against volume
 * loss.
 */
public final class DiskBuffer implements AutoCloseable {
  public static final int MAX_BATCH_RECORDS = 256;
  public static final long MAX_BATCH_BYTES = 4L * 1024 * 1024;
  private static final int MAGIC = 0x52545450;
  private static final int GROUP_MAGIC = 0x52544750;
  private static final int GROUP_HEADER_BYTES = 16;
  private static final int HEADER_BYTES =
      Integer.BYTES + Short.BYTES + Long.BYTES * 2 + Integer.BYTES * 2;
  private final Path directory;
  private final PlatformSettings.Buffer limits;
  private final FileChannel lockChannel;
  private final FileLock processLock;
  private final Durability durability;
  // Never acquire an operation lock while holding the state monitor. Close takes them in this
  // order so the volume lock cannot be released while either protocol is using the directory.
  private final Object appendLock = new Object();
  private final Object checkpointLock = new Object();

  interface Durability {
    void writeForced(Path path, byte[] content) throws IOException;

    void forceDirectory(Path directory) throws IOException;
  }

  static final Durability FILESYSTEM =
      new Durability() {
        @Override
        public void writeForced(Path path, byte[] content) throws IOException {
          try (var channel =
              FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer data = ByteBuffer.wrap(content);
            while (data.hasRemaining()) channel.write(data);
            channel.force(true);
          }
          if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
          }
        }

        @Override
        public void forceDirectory(Path directory) throws IOException {
          try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
          }
        }
      };

  private record Pending(Path path, Instant createdAt, long bytes, long offset) {}

  private record Allocation(long bytes, int references) {}

  private final TreeMap<Long, Pending> pending = new TreeMap<>();
  // A partially acknowledged group retains its entire physical file until its last member drains.
  private final Map<Path, Allocation> allocations = new HashMap<>();
  private long usedBytes;
  private long deliveredSequence;
  private long nextSequence = 1;
  private boolean closed;

  public record Entry(
      long sequence,
      Instant createdAt,
      String key,
      byte[] payload,
      Map<String, String> traceHeaders) {
    public Entry {
      payload = payload.clone();
      traceHeaders = Map.copyOf(traceHeaders);
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  public record Snapshot(
      int records,
      long bytes,
      long maxBytes,
      int maxRecords,
      long deliveredSequence,
      long oldestAgeMillis) {}

  /** A defensively copied append intent. */
  public record Append(String key, byte[] payload, Map<String, String> traceHeaders) {
    public Append {
      if (key == null || key.length() > 256 || key.isBlank() || payload == null)
        throw new IllegalArgumentException("Invalid buffered record");
      validateTraceHeaders(traceHeaders);
      payload = payload.clone();
      traceHeaders = Map.copyOf(traceHeaders);
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  private record Prepared(Entry entry, byte[] frame, Path temporary, Path committed) {}

  public DiskBuffer(Path directory, PlatformSettings.Buffer limits) throws IOException {
    this(directory, limits, FILESYSTEM);
  }

  DiskBuffer(Path directory, PlatformSettings.Buffer limits, Durability durability)
      throws IOException {
    this.directory = directory.toAbsolutePath().normalize();
    this.limits = limits;
    this.durability = java.util.Objects.requireNonNull(durability);
    Files.createDirectories(this.directory);
    if (Files.getFileStore(this.directory).supportsFileAttributeView("posix")) {
      Files.setPosixFilePermissions(this.directory, PosixFilePermissions.fromString("rwx------"));
    }
    lockChannel =
        FileChannel.open(
            this.directory.resolve(".lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    FileLock acquired;
    try {
      acquired = lockChannel.tryLock();
      if (acquired == null)
        throw new IOException("Buffer volume is already owned by another process");
    } catch (IOException | OverlappingFileLockException error) {
      lockChannel.close();
      throw new IOException("Cannot acquire exclusive buffer ownership", error);
    }
    processLock = acquired;
    try {
      recover();
    } catch (IOException | RuntimeException error) {
      processLock.release();
      lockChannel.close();
      throw error;
    }
  }

  public Entry append(String key, byte[] payload, Map<String, String> traceHeaders)
      throws IOException {
    return appendBatch(List.of(new Append(key, payload, traceHeaders))).getFirst();
  }

  /**
   * One forced, atomically published file holds a bounded group of independently checked frames.
   */
  public List<Entry> appendBatch(List<Append> records) throws IOException {
    return appendBatch(records, entries -> {});
  }

  /**
   * Register waiters after synchronization but before dispatcher visibility; no subscriber calls.
   */
  public List<Entry> appendBatch(
      List<Append> records, java.util.function.Consumer<List<Entry>> beforeVisible)
      throws IOException {
    java.util.Objects.requireNonNull(beforeVisible);
    if (records == null || records.isEmpty() || records.size() > MAX_BATCH_RECORDS)
      throw new IllegalArgumentException("Invalid append batch");
    records = List.copyOf(records);
    synchronized (appendLock) {
      long sequence;
      synchronized (this) {
        requireOpen();
        sequence = nextSequence;
      }
      var prepared = new ArrayList<Prepared>();
      long bytes = 0;
      for (var record : records) {
        if (record.payload.length > limits.maxRecordBytes())
          throw new IllegalArgumentException("Payload exceeds record limit");
        var entry =
            new Entry(sequence++, Instant.now(), record.key, record.payload, record.traceHeaders);
        byte[] frame =
            encode(
                entry.sequence(),
                entry.createdAt(),
                entry.key(),
                entry.payload,
                entry.traceHeaders());
        if (frame.length > limits.maxRecordBytes()
            || records.size() > 1 && frame.length > MAX_BATCH_BYTES - bytes)
          throw new IllegalArgumentException("Frame or append batch exceeds the byte limit");
        bytes += frame.length;
        prepared.add(
            new Prepared(
                entry,
                frame,
                directory.resolve(String.format("%020d.tmp", entry.sequence())),
                directory.resolve(String.format("%020d.evt", entry.sequence()))));
      }
      byte[] group = prepared.size() > 1 ? encodeGroup(prepared) : null;
      long physicalBytes = group == null ? bytes : group.length;
      synchronized (this) {
        if (prepared.size() > limits.maxRecords() - pending.size()
            || physicalBytes > limits.maxBytes() - usedBytes) throw new Full();
      }
      if (group != null) return appendGroup(prepared, group, beforeVisible);
      // Only this appender can consume budget; concurrent checkpoints can only release it.
      var committed = new ArrayList<Prepared>();
      boolean allWritten = false;
      try {
        for (var item : prepared) {
          durability.writeForced(item.temporary(), item.frame());
          Files.move(item.temporary(), item.committed(), StandardCopyOption.ATOMIC_MOVE);
          committed.add(item);
        }
        allWritten = true;
      } finally {
        try {
          if (!committed.isEmpty()) {
            try {
              durability.forceDirectory(directory);
              if (allWritten)
                beforeVisible.accept(committed.stream().map(Prepared::entry).toList());
            } finally {
              // A partial write or failed directory sync has ambiguous client outcomes. Charge
              // every renamed file and expose that prefix only after its sync attempt finishes.
              synchronized (this) {
                for (var item : committed) {
                  var entry = item.entry();
                  pending.put(
                      entry.sequence(),
                      new Pending(item.committed(), entry.createdAt(), item.frame().length, 0));
                  allocations.put(item.committed(), new Allocation(item.frame().length, 1));
                  usedBytes += item.frame().length;
                  nextSequence = entry.sequence() + 1;
                }
                notifyAll();
              }
            }
          }
        } finally {
          for (var item : prepared) Files.deleteIfExists(item.temporary());
        }
      }
      return prepared.stream().map(Prepared::entry).toList();
    }
  }

  private byte[] encodeGroup(List<Prepared> prepared) throws IOException {
    var body = new ByteArrayOutputStream();
    try (var output = new DataOutputStream(body)) {
      for (var item : prepared) {
        if ((long) GROUP_HEADER_BYTES + body.size() + Integer.BYTES + item.frame().length
            > MAX_BATCH_BYTES)
          throw new IllegalArgumentException("Append group exceeds the byte limit");
        output.writeInt(item.frame().length);
        output.write(item.frame());
      }
    }
    byte[] content = body.toByteArray();
    var crc = new CRC32C();
    crc.update(content);
    return ByteBuffer.allocate(GROUP_HEADER_BYTES + content.length)
        .putInt(GROUP_MAGIC)
        .putInt(1)
        .putInt(prepared.size())
        .putInt((int) crc.getValue())
        .put(content)
        .array();
  }

  private List<Entry> appendGroup(
      List<Prepared> prepared, byte[] group, java.util.function.Consumer<List<Entry>> beforeVisible)
      throws IOException {
    String name =
        String.format(
            "%020d-%020d",
            prepared.getFirst().entry().sequence(), prepared.getLast().entry().sequence());
    Path temporary = directory.resolve(name + ".tmp");
    Path committed = directory.resolve(name + ".grp");
    var entries = prepared.stream().map(Prepared::entry).toList();
    boolean renamed = false;
    try {
      durability.writeForced(temporary, group);
      Files.move(temporary, committed, StandardCopyOption.ATOMIC_MOVE);
      renamed = true;
      try {
        durability.forceDirectory(directory);
        beforeVisible.accept(entries);
      } finally {
        synchronized (this) {
          long offset = GROUP_HEADER_BYTES;
          for (var item : prepared) {
            offset += Integer.BYTES;
            var entry = item.entry();
            pending.put(
                entry.sequence(),
                new Pending(committed, entry.createdAt(), item.frame().length, offset));
            offset += item.frame().length;
          }
          allocations.put(committed, new Allocation(group.length, prepared.size()));
          usedBytes += group.length;
          nextSequence = entries.getLast().sequence() + 1;
          notifyAll();
        }
      }
      return entries;
    } finally {
      // An incomplete temporary group has never been acknowledged; a renamed group remains charged.
      if (!renamed) Files.deleteIfExists(temporary);
    }
  }

  public Optional<Entry> peek() throws IOException {
    synchronized (checkpointLock) {
      Pending head;
      synchronized (this) {
        requireOpen();
        head = pending.isEmpty() ? null : pending.firstEntry().getValue();
      }
      return head == null ? Optional.empty() : Optional.of(readPending(head));
    }
  }

  /** Read a bounded prefix. A large head is returned alone so a valid record cannot be stranded. */
  public List<Entry> peekBatch(int maxRecords, long maxBytes) throws IOException {
    if (maxRecords < 1
        || maxRecords > MAX_BATCH_RECORDS
        || maxBytes < 1
        || maxBytes > MAX_BATCH_BYTES)
      throw new IllegalArgumentException("Invalid spool batch limits");
    synchronized (checkpointLock) {
      var selected = new ArrayList<Pending>();
      synchronized (this) {
        requireOpen();
        long bytes = 0;
        for (var record : pending.values()) {
          if (selected.size() == maxRecords
              || (!selected.isEmpty() && record.bytes() > maxBytes - bytes)) break;
          selected.add(record);
          bytes += record.bytes();
        }
      }
      var result = new ArrayList<Entry>();
      for (var record : selected) result.add(readPending(record));
      return List.copyOf(result);
    }
  }

  /** Wait without polling or holding the spool monitor across a sleep. */
  public synchronized boolean awaitRecords(Duration deadline)
      throws IOException, InterruptedException {
    if (deadline == null || deadline.isNegative() || deadline.compareTo(Duration.ofSeconds(1)) > 0)
      throw new IllegalArgumentException("Invalid spool wait deadline");
    long remaining = deadline.toNanos();
    long started = System.nanoTime();
    while (pending.isEmpty() && !closed && remaining > 0) {
      java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
      remaining = deadline.toNanos() - (System.nanoTime() - started);
    }
    requireOpen();
    return !pending.isEmpty();
  }

  /** Only the head can advance. Persist the delivered checkpoint before reclaiming its file. */
  public void acknowledge(long sequence) throws IOException {
    acknowledgeBatch(List.of(sequence));
  }

  /** Checkpoint only a fully confirmed prefix; one durable checkpoint covers the complete batch. */
  public void acknowledgeBatch(List<Long> sequences) throws IOException {
    if (sequences == null || sequences.isEmpty() || sequences.size() > MAX_BATCH_RECORDS)
      throw new IllegalArgumentException("Invalid checkpoint batch");
    sequences = new ArrayList<>(sequences);
    synchronized (checkpointLock) {
      long sequence;
      boolean alreadyDelivered;
      synchronized (this) {
        requireOpen();
        long previous = 0;
        var unconfirmed = pending.tailMap(deliveredSequence, false).keySet().iterator();
        for (Long candidate : sequences) {
          if (candidate == null || candidate <= previous)
            throw new IllegalArgumentException("Checkpoint sequences must increase");
          previous = candidate;
          if (candidate > deliveredSequence
              && (!unconfirmed.hasNext() || unconfirmed.next().longValue() != candidate))
            throw new IllegalArgumentException("Out-of-order acknowledgement prefix");
        }
        sequence = previous;
        alreadyDelivered = sequence <= deliveredSequence;
      }
      if (!alreadyDelivered) writeCheckpoint(sequence);
      reclaimDelivered();
    }
  }

  private void writeCheckpoint(long sequence) throws IOException {
    Path checkpointTemporary = directory.resolve("checkpoint.tmp");
    byte[] sequenceBytes = ByteBuffer.allocate(Long.BYTES).putLong(sequence).array();
    var crc = new CRC32C();
    crc.update(sequenceBytes);
    byte[] checkpoint =
        ByteBuffer.allocate(16)
            .putInt(MAGIC)
            .put(sequenceBytes)
            .putInt((int) crc.getValue())
            .array();
    try {
      durability.writeForced(checkpointTemporary, checkpoint);
      Files.move(
          checkpointTemporary,
          directory.resolve("checkpoint"),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      durability.forceDirectory(directory);
    } finally {
      Files.deleteIfExists(checkpointTemporary);
    }
    synchronized (this) {
      deliveredSequence = sequence;
    }
  }

  /** A durable checkpoint and successful file reclamation are separate states. */
  private void reclaimDelivered() throws IOException {
    boolean removed = false;
    for (; ; ) {
      Map.Entry<Long, Pending> head;
      synchronized (this) {
        head = pending.firstEntry();
        if (head == null || head.getKey() > deliveredSequence) break;
        var allocation = allocations.get(head.getValue().path());
        if (allocation.references() > 1) {
          pending.remove(head.getKey());
          allocations.put(
              head.getValue().path(),
              new Allocation(allocation.bytes(), allocation.references() - 1));
          continue;
        }
      }
      // A prior attempt may have deleted the file before its directory sync failed.
      Files.deleteIfExists(head.getValue().path());
      synchronized (this) {
        pending.remove(head.getKey());
        usedBytes -= allocations.remove(head.getValue().path()).bytes();
      }
      removed = true;
    }
    if (removed) durability.forceDirectory(directory);
  }

  public synchronized Snapshot snapshot() {
    long age = 0;
    if (!pending.isEmpty()) {
      age =
          Math.max(
              0,
              Instant.now().toEpochMilli()
                  - pending.firstEntry().getValue().createdAt().toEpochMilli());
    }
    return new Snapshot(
        pending.size(), usedBytes, limits.maxBytes(), limits.maxRecords(), deliveredSequence, age);
  }

  public int maxRecordBytes() {
    return limits.maxRecordBytes();
  }

  public void validateAppend(Append record) throws IOException {
    if (record.payload.length > limits.maxRecordBytes()
        || encode(1, Instant.EPOCH, record.key, record.payload, record.traceHeaders).length
            > limits.maxRecordBytes())
      throw new IllegalArgumentException("Frame exceeds record limit");
  }

  private void recover() throws IOException {
    Path checkpoint = directory.resolve("checkpoint");
    if (Files.exists(checkpoint, LinkOption.NOFOLLOW_LINKS)) {
      if (!Files.isRegularFile(checkpoint, LinkOption.NOFOLLOW_LINKS)
          || Files.size(checkpoint) != 16) throw new Corrupt("Invalid delivered checkpoint");
      ByteBuffer data = ByteBuffer.wrap(Files.readAllBytes(checkpoint));
      if (data.getInt() != MAGIC) throw new Corrupt("Invalid checkpoint magic");
      byte[] sequenceBytes = new byte[Long.BYTES];
      data.get(sequenceBytes);
      var crc = new CRC32C();
      crc.update(sequenceBytes);
      if ((int) crc.getValue() != data.getInt()) throw new Corrupt("Checkpoint checksum mismatch");
      deliveredSequence = ByteBuffer.wrap(sequenceBytes).getLong();
      if (deliveredSequence < 0) throw new Corrupt("Negative delivered checkpoint");
    }
    try (var paths = Files.list(directory)) {
      for (Path path : paths.toList()) {
        String name = path.getFileName().toString();
        if (name.endsWith(".tmp")) {
          Files.delete(path); // Incomplete append/checkpoint never acknowledged to the HTTP caller.
        } else if (name.matches("[0-9]{20}\\.evt")) {
          if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
            throw new Corrupt("Unexpected spool entry type");
          Entry entry = read(path); // Every committed entry is validated before admission reopens.
          long filenameSequence;
          try {
            filenameSequence = Long.parseLong(name.substring(0, 20));
          } catch (NumberFormatException invalid) {
            throw new Corrupt("Invalid spool sequence");
          }
          if (entry.sequence() != filenameSequence) throw new Corrupt("Spool sequence mismatch");
          if (entry.sequence() <= deliveredSequence) {
            Files.delete(path); // Crash after checkpoint, before file reclamation.
          } else {
            long bytes = Files.size(path);
            if (pending.putIfAbsent(
                    entry.sequence(), new Pending(path, entry.createdAt(), bytes, 0))
                != null) throw new Corrupt("Overlapping spool entries");
            allocations.put(path, new Allocation(bytes, 1));
            usedBytes += bytes;
          }
        } else if (name.matches("[0-9]{20}-[0-9]{20}\\.grp")) {
          recoverGroup(path);
        } else if (!name.equals(".lock") && !name.equals("checkpoint")) {
          throw new Corrupt("Unexpected file in private buffer directory");
        }
      }
    }
    if (pending.size() > limits.maxRecords() || usedBytes > limits.maxBytes()) {
      throw new IOException(
          "Existing backlog exceeds configured budget; restore its original limits to drain");
    }
    nextSequence = Math.max(deliveredSequence, pending.isEmpty() ? 0 : pending.lastKey()) + 1;
    durability.forceDirectory(directory);
  }

  private void recoverGroup(Path path) throws IOException {
    long size = Files.size(path);
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
        || size < GROUP_HEADER_BYTES
        || size > MAX_BATCH_BYTES) throw new Corrupt("Invalid append group size or type");
    ByteBuffer data = ByteBuffer.wrap(Files.readAllBytes(path));
    if (data.getInt() != GROUP_MAGIC || data.getInt() != 1)
      throw new Corrupt("Unsupported append group");
    int count = data.getInt();
    int checksum = data.getInt();
    if (count < 2 || count > MAX_BATCH_RECORDS) throw new Corrupt("Invalid append group count");
    var crc = new CRC32C();
    crc.update(data.asReadOnlyBuffer());
    if ((int) crc.getValue() != checksum) throw new Corrupt("Append group checksum mismatch");
    String name = path.getFileName().toString();
    long first;
    long last;
    try {
      first = Long.parseLong(name.substring(0, 20));
      last = Long.parseLong(name.substring(21, 41));
    } catch (NumberFormatException invalid) {
      throw new Corrupt("Invalid append group sequence");
    }
    if (first < 1 || last < first || last - first != count - 1)
      throw new Corrupt("Append group sequence range mismatch");
    int retained = 0;
    for (int index = 0; index < count; index++) {
      if (data.remaining() < Integer.BYTES) throw new Corrupt("Truncated append group");
      int length = data.getInt();
      if (length < HEADER_BYTES || length > limits.maxRecordBytes() || length > data.remaining())
        throw new Corrupt("Invalid append group frame size");
      long offset = data.position();
      var frame = data.slice();
      frame.limit(length);
      Entry entry = decode(frame);
      if (entry.sequence() != first + index) throw new Corrupt("Append group sequence mismatch");
      data.position(data.position() + length);
      if (entry.sequence() > deliveredSequence) {
        if (pending.putIfAbsent(
                entry.sequence(), new Pending(path, entry.createdAt(), length, offset))
            != null) throw new Corrupt("Overlapping spool entries");
        retained++;
      }
    }
    if (data.hasRemaining()) throw new Corrupt("Trailing append group data");
    if (retained == 0) {
      Files.delete(path);
    } else {
      allocations.put(path, new Allocation(size, retained));
      usedBytes += size;
    }
  }

  private byte[] encode(
      long sequence, Instant created, String key, byte[] payload, Map<String, String> headers)
      throws IOException {
    var body = new ByteArrayOutputStream();
    try (var output = new DataOutputStream(body)) {
      output.writeUTF(key);
      output.writeInt(headers.size());
      for (var header : new TreeMap<>(headers).entrySet()) {
        output.writeUTF(header.getKey());
        output.writeUTF(header.getValue());
      }
      output.writeInt(payload.length);
      output.write(payload);
    }
    byte[] content = body.toByteArray();
    var crc = new CRC32C();
    crc.update(content);
    return ByteBuffer.allocate(HEADER_BYTES + content.length)
        .putInt(MAGIC)
        .putShort((short) 1)
        .putLong(sequence)
        .putLong(created.toEpochMilli())
        .putInt(content.length)
        .putInt((int) crc.getValue())
        .put(content)
        .array();
  }

  private Entry read(Path path) throws IOException {
    long size = Files.size(path);
    if (size < HEADER_BYTES || size > limits.maxRecordBytes())
      throw new Corrupt("Invalid spool frame size");
    return decode(ByteBuffer.wrap(Files.readAllBytes(path)));
  }

  private Entry readPending(Pending pending) throws IOException {
    if (pending.offset() == 0) return read(pending.path());
    var frame = ByteBuffer.allocate(Math.toIntExact(pending.bytes()));
    try (var channel = FileChannel.open(pending.path(), StandardOpenOption.READ)) {
      channel.position(pending.offset());
      while (frame.hasRemaining()) {
        if (channel.read(frame) < 0) throw new Corrupt("Truncated append group frame");
      }
    }
    frame.flip();
    return decode(frame);
  }

  private Entry decode(ByteBuffer frame) throws IOException {
    if (frame.getInt() != MAGIC || frame.getShort() != 1)
      throw new Corrupt("Unsupported spool frame");
    long sequence = frame.getLong();
    long created = frame.getLong();
    int length = frame.getInt();
    int checksum = frame.getInt();
    if (sequence < 1 || length != frame.remaining()) throw new Corrupt("Invalid spool header");
    byte[] body = new byte[length];
    frame.get(body);
    var crc = new CRC32C();
    crc.update(body);
    if ((int) crc.getValue() != checksum) throw new Corrupt("Spool checksum mismatch");
    try (var input = new DataInputStream(new ByteArrayInputStream(body))) {
      String key = input.readUTF();
      int count = input.readInt();
      if (count < 0 || count > 2) throw new Corrupt("Invalid trace metadata");
      var headers = new HashMap<String, String>();
      for (int index = 0; index < count; index++) {
        if (headers.put(input.readUTF(), input.readUTF()) != null)
          throw new Corrupt("Duplicate trace metadata");
      }
      validateTraceHeaders(headers);
      int payloadLength = input.readInt();
      if (payloadLength < 0 || payloadLength != input.available())
        throw new Corrupt("Invalid spool payload size");
      byte[] payload = input.readNBytes(payloadLength);
      return new Entry(sequence, Instant.ofEpochMilli(created), key, payload, headers);
    } catch (IllegalArgumentException invalid) {
      throw new Corrupt("Invalid spool metadata");
    }
  }

  private static void validateTraceHeaders(Map<String, String> headers) {
    if (headers == null || headers.size() > 2)
      throw new IllegalArgumentException("Invalid trace metadata");
    headers.forEach(
        (key, value) -> {
          if ((!key.equals("traceparent") && !key.equals("tracestate"))
              || value == null
              || value.length() > 512
              || value.chars().anyMatch(c -> c < 32 || c > 126)) {
            throw new IllegalArgumentException("Unsupported trace metadata");
          }
        });
  }

  private void requireOpen() throws IOException {
    if (closed) throw new IOException("Buffer is closed");
  }

  @Override
  public void close() throws IOException {
    synchronized (appendLock) {
      synchronized (checkpointLock) {
        synchronized (this) {
          if (closed) return;
          closed = true;
          notifyAll();
        }
        processLock.release();
        lockChannel.close();
      }
    }
  }

  public static final class Full extends IOException {
    private static final long serialVersionUID = 1L;

    public Full() {
      super("Durable buffer capacity exhausted");
    }
  }

  public static final class Corrupt extends IOException {
    private static final long serialVersionUID = 1L;

    public Corrupt(String message) {
      super(message);
    }
  }
}
