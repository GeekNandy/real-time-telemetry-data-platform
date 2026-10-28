package dev.sharad.telemetry.buffer;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.config.PlatformSettings;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskBufferTest {
  @TempDir Path directory;
  private final PlatformSettings.Buffer limits =
      new PlatformSettings.Buffer(1024 * 1024, 1000, 4096);

  @Test
  void reopensAnAcknowledgedDurableAppend() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.append("a:id", bytes("hello"), Map.of("traceparent", "test-context"));
    }
    try (var buffer = new DiskBuffer(directory, limits)) {
      var entry = buffer.peek().orElseThrow();
      assertEquals("a:id", entry.key());
      assertArrayEquals(bytes("hello"), entry.payload());
      assertEquals("test-context", entry.traceHeaders().get("traceparent"));
      assertEquals(1, buffer.snapshot().records());
    }
  }

  @Test
  void checkpointReclaimsSpaceAndMaintainsSequenceAcrossRestart() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      long sequence = buffer.append("a:id", bytes("one"), Map.of()).sequence();
      buffer.acknowledge(sequence);
      assertEquals(0, buffer.snapshot().bytes());
    }
    try (var buffer = new DiskBuffer(directory, limits)) {
      assertTrue(buffer.peek().isEmpty());
      assertEquals(2, buffer.append("a:next", bytes("two"), Map.of()).sequence());
    }
  }

  @Test
  void enforcesRecordCountAndReleasesBudgetAfterDelivery() throws Exception {
    var budget = new PlatformSettings.Buffer(4096, 1, 1024);
    try (var buffer = new DiskBuffer(directory, budget)) {
      var first = buffer.append("a:id", bytes("one"), Map.of());
      assertThrows(DiskBuffer.Full.class, () -> buffer.append("b:id", bytes("two"), Map.of()));
      buffer.acknowledge(first.sequence());
      assertDoesNotThrow(() -> buffer.append("b:id", bytes("two"), Map.of()));
    }
  }

  @Test
  void enforcesTheByteBudget() throws Exception {
    var budget = new PlatformSettings.Buffer(256, 100, 256);
    try (var buffer = new DiskBuffer(directory, budget)) {
      buffer.append("a:id", new byte[100], Map.of());
      assertThrows(DiskBuffer.Full.class, () -> buffer.append("b:id", new byte[100], Map.of()));
    }
  }

  @Test
  void rejectsOutOfOrderDeliveryCheckpoint() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      var a = buffer.append("a:id", bytes("one"), Map.of());
      var b = buffer.append("b:id", bytes("two"), Map.of());
      assertThrows(IllegalArgumentException.class, () -> buffer.acknowledge(b.sequence()));
      buffer.acknowledge(a.sequence());
      buffer.acknowledge(a.sequence());
      buffer.acknowledge(b.sequence());
      assertTrue(buffer.peek().isEmpty());
    }
  }

  @Test
  void refusesSharedVolumeOwnership() throws Exception {
    try (var first = new DiskBuffer(directory, limits)) {
      assertThrows(java.io.IOException.class, () -> new DiskBuffer(directory, limits));
    }
  }

  @Test
  void removesUnacknowledgedPartialFilesOnRestart() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.append("a:id", bytes("one"), Map.of());
    }
    Files.write(directory.resolve("00000000000000000002.tmp"), new byte[] {1, 2, 3});
    try (var buffer = new DiskBuffer(directory, limits)) {
      assertEquals(1, buffer.snapshot().records());
      assertFalse(Files.exists(directory.resolve("00000000000000000002.tmp")));
    }
  }

  @Test
  void failsClosedOnCorruptionOfACommittedRecord() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.append("a:id", bytes("one"), Map.of());
    }
    Path record = directory.resolve("00000000000000000001.evt");
    byte[] data = Files.readAllBytes(record);
    data[data.length - 1] ^= 1;
    Files.write(record, data);
    assertThrows(DiskBuffer.Corrupt.class, () -> new DiskBuffer(directory, limits));
    assertTrue(Files.exists(record), "Corrupt acknowledged data must not be silently deleted");
  }

  @Test
  void validatesAndCopiesTraceMetadataAndPayload() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      assertThrows(
          IllegalArgumentException.class,
          () -> buffer.append("a:id", bytes("one"), Map.of("Authorization", "never-store-this")));
      byte[] payload = bytes("one");
      var entry = buffer.append("a:id", payload, Map.of());
      payload[0] = 0;
      entry.payload()[0] = 0;
      assertArrayEquals(bytes("one"), buffer.peek().orElseThrow().payload());
    }
  }

  @Test
  void refusesACorruptCheckpointBeforeDiscardingAnyPendingData() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      var first = buffer.append("a:first", bytes("one"), Map.of());
      buffer.acknowledge(first.sequence());
      buffer.append("a:second", bytes("two"), Map.of());
    }
    Path checkpoint = directory.resolve("checkpoint");
    byte[] data = Files.readAllBytes(checkpoint);
    data[8] ^= 1;
    Files.write(checkpoint, data);
    assertThrows(DiskBuffer.Corrupt.class, () -> new DiskBuffer(directory, limits));
    assertTrue(Files.exists(directory.resolve("00000000000000000002.evt")));
  }

  @Test
  void serializesConcurrentDurableAppendsWithoutLosingRecords() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits);
        var pool = Executors.newFixedThreadPool(8)) {
      var tasks = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
      for (int i = 0; i < 100; i++) {
        int id = i;
        tasks.add(
            pool.submit(() -> buffer.append("a:" + id, bytes("value-" + id), Map.of()).sequence()));
      }
      var sequences = new java.util.HashSet<Long>();
      for (var task : tasks) sequences.add(task.get(10, TimeUnit.SECONDS));
      assertEquals(100, sequences.size());
      assertEquals(100, buffer.snapshot().records());
    }
  }

  @Test
  void survivesForcedProcessTerminationAfterAcknowledgement() throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    Process child =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                BufferCrashProbe.class.getName(),
                directory.toString())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    try {
      var reader =
          new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
      String ack = assertTimeoutPreemptively(Duration.ofSeconds(10), reader::readLine);
      assertEquals("ACK 1", ack);
      child.destroyForcibly();
      assertTrue(child.waitFor(5, TimeUnit.SECONDS));
      try (var recovered = new DiskBuffer(directory, limits)) {
        assertArrayEquals(bytes("accepted-before-kill"), recovered.peek().orElseThrow().payload());
      }
    } finally {
      child.destroyForcibly();
      child.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void retriesReclamationAfterTheDeliveredCheckpointIsCommitted() throws Exception {
    Path spool = directory.resolve("spool");
    try (var buffer = new DiskBuffer(spool, limits)) {
      var first = buffer.append("a:first", bytes("one"), Map.of());
      var second = buffer.append("a:second", bytes("two"), Map.of());
      Path record = spool.resolve(String.format("%020d.evt", first.sequence()));
      Path saved = directory.resolve("original.evt");
      // Inject an unlink failure after checkpointing without changing production I/O paths.
      Files.move(record, saved);
      Files.createDirectory(record);
      Files.writeString(record.resolve("fault"), "prevent deletion");
      assertThrows(
          java.nio.file.DirectoryNotEmptyException.class,
          () -> buffer.acknowledge(first.sequence()));
      assertEquals(first.sequence(), buffer.snapshot().deliveredSequence());
      assertEquals(2, buffer.snapshot().records());
      Files.delete(record.resolve("fault"));
      Files.delete(record);
      Files.move(saved, record);

      buffer.acknowledge(first.sequence());
      assertEquals(second.sequence(), buffer.peek().orElseThrow().sequence());
      assertEquals(1, buffer.snapshot().records());
      buffer.acknowledge(second.sequence());
      assertTrue(buffer.peek().isEmpty());
      assertEquals(0, buffer.snapshot().bytes());
    }
    try (var recovered = new DiskBuffer(spool, limits)) {
      assertTrue(recovered.peek().isEmpty());
      assertEquals(3, recovered.append("a:third", bytes("three"), Map.of()).sequence());
    }
  }

  @Test
  void reclaimsADeliveredRecordWhoseFileIsAlreadyAbsent() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      var first = buffer.append("a:first", bytes("one"), Map.of());
      var second = buffer.append("a:second", bytes("two"), Map.of());
      Files.delete(directory.resolve(String.format("%020d.evt", first.sequence())));
      buffer.acknowledge(first.sequence());
      assertEquals(second.sequence(), buffer.peek().orElseThrow().sequence());
      assertEquals(1, buffer.snapshot().records());
    }
  }

  @Test
  void boundsBatchReadsWithoutSkippingTheHead() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      var first = buffer.append("a:one", bytes("one"), Map.of());
      var second = buffer.append("a:two", bytes("two"), Map.of());
      buffer.append("a:three", bytes("three"), Map.of());
      assertEquals(
          List.of(first.sequence(), second.sequence()),
          buffer.peekBatch(2, 4096).stream().map(DiskBuffer.Entry::sequence).toList());
      long headBytes = Files.size(directory.resolve(String.format("%020d.evt", first.sequence())));
      assertEquals(1, buffer.peekBatch(10, headBytes).size());
      assertEquals(first.sequence(), buffer.peekBatch(10, 1).getFirst().sequence());
      assertThrows(IllegalArgumentException.class, () -> buffer.peekBatch(257, 4096));
      assertThrows(IllegalArgumentException.class, () -> buffer.peekBatch(1, 0));
    }
  }

  @Test
  void checkpointsOnlyACompletePrefixAndRecoversItsRemainder() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      for (int id = 0; id < 3; id++) buffer.append("a:" + id, bytes("data"), Map.of());
      assertThrows(IllegalArgumentException.class, () -> buffer.acknowledgeBatch(List.of(1L, 3L)));
      assertThrows(IllegalArgumentException.class, () -> buffer.acknowledgeBatch(List.of(1L, 1L)));
      assertThrows(IllegalArgumentException.class, () -> buffer.acknowledgeBatch(List.of(2L, 1L)));
      assertEquals(0, buffer.snapshot().deliveredSequence());
      buffer.acknowledgeBatch(List.of(1L, 2L));
      assertEquals(2, buffer.snapshot().deliveredSequence());
      assertEquals(3, buffer.peek().orElseThrow().sequence());
    }
    try (var recovered = new DiskBuffer(directory, limits)) {
      assertEquals(1, recovered.snapshot().records());
      recovered.acknowledgeBatch(List.of(1L, 2L));
      assertEquals(3, recovered.peek().orElseThrow().sequence());
      recovered.acknowledge(3);
      assertTrue(recovered.peek().isEmpty());
    }
  }

  @Test
  void failedBatchCheckpointRetainsEveryRecord() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      for (int id = 0; id < 3; id++) buffer.append("a:" + id, bytes("data"), Map.of());
      Files.createDirectory(directory.resolve("checkpoint"));
      assertThrows(java.io.IOException.class, () -> buffer.acknowledgeBatch(List.of(1L, 2L, 3L)));
      assertEquals(0, buffer.snapshot().deliveredSequence());
      assertEquals(3, buffer.snapshot().records());
      Files.delete(directory.resolve("checkpoint"));
      buffer.acknowledgeBatch(List.of(1L, 2L, 3L));
      assertEquals(0, buffer.snapshot().records());
    }
  }

  @Test
  void retriesBatchReclamationAfterItsDurableCheckpoint() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      for (int id = 0; id < 3; id++) buffer.append("a:" + id, bytes("data"), Map.of());
      Path record = directory.resolve("00000000000000000001.evt");
      Path saved = directory.resolveSibling(directory.getFileName() + "-saved.evt");
      Files.move(record, saved);
      try {
        Files.createDirectory(record);
        Files.writeString(record.resolve("fault"), "unlink failure");
        assertThrows(
            java.nio.file.DirectoryNotEmptyException.class,
            () -> buffer.acknowledgeBatch(List.of(1L, 2L)));
        assertEquals(2, buffer.snapshot().deliveredSequence());
        assertEquals(3, buffer.snapshot().records());
        Files.delete(record.resolve("fault"));
        Files.delete(record);
        Files.move(saved, record);
        buffer.acknowledgeBatch(List.of(1L, 2L));
        assertEquals(3, buffer.peek().orElseThrow().sequence());
        assertEquals(1, buffer.snapshot().records());
      } finally {
        Files.deleteIfExists(saved);
      }
    }
  }

  @Test
  void dataNotificationAndCloseCannotStrandAWaiter() throws Exception {
    var buffer = new DiskBuffer(directory, limits);
    try (var pool = Executors.newSingleThreadExecutor()) {
      assertFalse(buffer.awaitRecords(Duration.ZERO));
      var ready = new java.util.concurrent.CountDownLatch(1);
      var waiting =
          pool.submit(
              () -> {
                ready.countDown();
                while (!buffer.awaitRecords(Duration.ofSeconds(1))) {}
                return true;
              });
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      buffer.append("a:notify", bytes("data"), Map.of());
      assertTrue(waiting.get(5, TimeUnit.SECONDS));
      buffer.acknowledge(1);
      var closed =
          pool.submit(
              () -> {
                while (!buffer.awaitRecords(Duration.ofSeconds(1))) {}
                return true;
              });
      buffer.close();
      assertInstanceOf(
          java.io.IOException.class,
          assertThrows(
                  java.util.concurrent.ExecutionException.class,
                  () -> closed.get(5, TimeUnit.SECONDS))
              .getCause());
    } finally {
      buffer.close();
    }
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }
}
