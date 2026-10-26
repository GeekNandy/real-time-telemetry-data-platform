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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/**
 * A bounded file-per-record spool for a persistent local volume. Append completes after file fsync,
 * atomic rename and directory fsync. The single dispatcher checkpoints only after a confirmed Kafka
 * acknowledgement. Survives process restart; this is not replication or protection against volume
 * loss.
 */
public final class DiskBuffer implements AutoCloseable {
  private static final int MAGIC = 0x52545450;
  private static final int HEADER_BYTES =
      Integer.BYTES + Short.BYTES + Long.BYTES * 2 + Integer.BYTES * 2;
  private final Path directory;
  private final PlatformSettings.Buffer limits;
  private final FileChannel lockChannel;
  private final FileLock processLock;

  private record Pending(Path path, Instant createdAt, long bytes) {}

  private final TreeMap<Long, Pending> pending = new TreeMap<>();
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

  public DiskBuffer(Path directory, PlatformSettings.Buffer limits) throws IOException {
    this.directory = directory.toAbsolutePath().normalize();
    this.limits = limits;
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

  public synchronized Entry append(String key, byte[] payload, Map<String, String> traceHeaders)
      throws IOException {
    requireOpen();
    if (key == null || key.length() > 256 || key.isBlank() || payload == null) {
      throw new IllegalArgumentException("Invalid buffered record");
    }
    validateTraceHeaders(traceHeaders);
    if (payload.length > limits.maxRecordBytes())
      throw new IllegalArgumentException("Payload exceeds record limit");
    long sequence = nextSequence;
    Instant created = Instant.now();
    byte[] frame = encode(sequence, created, key, payload, traceHeaders);
    if (frame.length > limits.maxRecordBytes())
      throw new IllegalArgumentException("Frame exceeds record limit");
    if (pending.size() >= limits.maxRecords() || frame.length > limits.maxBytes() - usedBytes)
      throw new Full();
    Path temporary = directory.resolve(String.format("%020d.tmp", sequence));
    Path committed = directory.resolve(String.format("%020d.evt", sequence));
    try {
      writeForced(temporary, frame);
      Files.move(temporary, committed, StandardCopyOption.ATOMIC_MOVE);
      // Account for the file even if the following directory sync fails. Its client outcome is
      // ambiguous.
      pending.put(sequence, new Pending(committed, created, frame.length));
      usedBytes += frame.length;
      nextSequence++;
      forceDirectory();
      return new Entry(sequence, created, key, payload, traceHeaders);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  public synchronized Optional<Entry> peek() throws IOException {
    requireOpen();
    return pending.isEmpty()
        ? Optional.empty()
        : Optional.of(read(pending.firstEntry().getValue().path()));
  }

  /** Only the head can advance. Persist the delivered checkpoint before reclaiming its file. */
  public synchronized void acknowledge(long sequence) throws IOException {
    requireOpen();
    if (sequence <= deliveredSequence) return;
    if (pending.isEmpty() || pending.firstKey() != sequence)
      throw new IllegalArgumentException("Out-of-order acknowledgement");
    Pending head = pending.firstEntry().getValue();
    Path record = head.path();
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
      writeForced(checkpointTemporary, checkpoint);
      Files.move(
          checkpointTemporary,
          directory.resolve("checkpoint"),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      forceDirectory();
    } finally {
      Files.deleteIfExists(checkpointTemporary);
    }
    deliveredSequence = sequence;
    Files.delete(record);
    pending.remove(sequence);
    usedBytes -= head.bytes();
    forceDirectory();
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
            pending.put(entry.sequence(), new Pending(path, entry.createdAt(), bytes));
            usedBytes += bytes;
          }
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
    forceDirectory();
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
    ByteBuffer frame = ByteBuffer.wrap(Files.readAllBytes(path));
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

  private void writeForced(Path path, byte[] content) throws IOException {
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

  private void forceDirectory() throws IOException {
    try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private void requireOpen() throws IOException {
    if (closed) throw new IOException("Buffer is closed");
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    processLock.release();
    lockChannel.close();
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
