package dev.sharad.telemetry.buffer;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.config.PlatformSettings;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DiskBufferGroupTest {
  @TempDir Path directory;
  private final PlatformSettings.Buffer limits = new PlatformSettings.Buffer(1_048_576, 128, 4096);

  static DiskBuffer.Append intent(String key) {
    return new DiskBuffer.Append(
        key, key.getBytes(StandardCharsets.UTF_8), Map.of("traceparent", "context"));
  }

  private Path group(Path spool) throws Exception {
    try (var files = Files.list(spool)) {
      return files.filter(p -> p.toString().endsWith(".grp")).findFirst().orElseThrow();
    }
  }

  @Test
  void repeatedMixedAppendsPartialCheckpointsAndRestartsMatchTheDurableLogModel() throws Exception {
    var model = new java.util.TreeMap<Long, String>();
    var random = new java.util.Random(17);
    var bounded = new PlatformSettings.Buffer(1_048_576, 32, 4096);
    DiskBuffer buffer = new DiskBuffer(directory, bounded);
    try {
      for (int step = 0; step < 150; step++) {
        int count = random.nextInt(1, 6);
        if (model.isEmpty() || model.size() + count <= 32 && random.nextBoolean()) {
          var intents = new java.util.ArrayList<DiskBuffer.Append>();
          for (int item = 0; item < count; item++) intents.add(intent("a:" + step + ":" + item));
          for (var entry : buffer.appendBatch(intents)) model.put(entry.sequence(), entry.key());
        } else {
          var prefix = model.keySet().stream().limit(random.nextInt(1, model.size() + 1)).toList();
          buffer.acknowledgeBatch(prefix);
          prefix.forEach(model::remove);
        }
        if (step % 5 == 0) {
          buffer.close();
          buffer = new DiskBuffer(directory, bounded);
        }
        assertEquals(
            model.values().stream().toList(),
            buffer.peekBatch(32, 1_048_576).stream().map(DiskBuffer.Entry::key).toList());
        assertEquals(model.size(), buffer.snapshot().records());
        long physical = 0;
        try (var files = Files.list(directory)) {
          for (var file : files.toList()) {
            if (file.toString().endsWith(".evt") || file.toString().endsWith(".grp"))
              physical += Files.size(file);
          }
        }
        assertEquals(physical, buffer.snapshot().bytes());
        assertTrue(physical <= bounded.maxBytes());
      }
    } finally {
      buffer.close();
    }
  }

  @Test
  void mixedLegacyAndGroupFilesRecoverOnlyUnconfirmedRecordsAndRetainTheirPhysicalBudget()
      throws Exception {
    long groupBytes;
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.append("a:legacy", new byte[] {0}, Map.of());
      buffer.appendBatch(List.of(intent("a:one"), intent("a:two"), intent("a:three")));
      groupBytes = Files.size(group(directory));
      buffer.acknowledgeBatch(List.of(1L, 2L));
      assertEquals(2, buffer.snapshot().records());
      assertEquals(groupBytes, buffer.snapshot().bytes());
      buffer.append("a:last", new byte[] {9}, Map.of());
    }
    try (var buffer = new DiskBuffer(directory, limits)) {
      assertEquals(
          List.of(3L, 4L, 5L),
          buffer.peekBatch(10, 4096).stream().map(DiskBuffer.Entry::sequence).toList());
      assertEquals("a:two", buffer.peek().orElseThrow().key());
      assertEquals("context", buffer.peek().orElseThrow().traceHeaders().get("traceparent"));
      long bytes = buffer.snapshot().bytes();
      buffer.acknowledge(3);
      assertEquals(bytes, buffer.snapshot().bytes());
      buffer.acknowledge(4);
      assertEquals(bytes - groupBytes, buffer.snapshot().bytes());
      assertEquals(6, buffer.append("a:next", new byte[] {6}, Map.of()).sequence());
      buffer.acknowledgeBatch(List.of(5L, 6L));
      assertEquals(0, buffer.snapshot().bytes());
    }
  }

  @Test
  void partiallyDeliveredGroupsCannotLendTheirRetainedDiskBytesToNewAdmissions() throws Exception {
    var small = new PlatformSettings.Buffer(1024, 10, 512);
    var item = new DiskBuffer.Append("a:item", new byte[300], Map.of());
    try (var buffer = new DiskBuffer(directory, small)) {
      buffer.appendBatch(List.of(item, item));
      long charged = buffer.snapshot().bytes();
      buffer.acknowledge(1);
      assertEquals(charged, buffer.snapshot().bytes());
      assertThrows(DiskBuffer.Full.class, () -> buffer.append("a:new", new byte[300], Map.of()));
    }
    try (var buffer = new DiskBuffer(directory, small)) {
      assertEquals(1, buffer.snapshot().records());
      assertThrows(DiskBuffer.Full.class, () -> buffer.append("a:new", new byte[300], Map.of()));
      buffer.acknowledge(2);
      assertEquals(3, buffer.append("a:new", new byte[300], Map.of()).sequence());
    }
  }

  @Test
  void failedFinalUnlinkKeepsPhysicalBytesChargedAndRetriesAfterTheDurableCheckpoint()
      throws Exception {
    Path spool = directory.resolve("spool");
    try (var buffer = new DiskBuffer(spool, limits)) {
      buffer.appendBatch(List.of(intent("a:one"), intent("a:two")));
      Path file = group(spool);
      long charged = buffer.snapshot().bytes();
      buffer.acknowledge(1);
      assertTrue(Files.isRegularFile(file));
      Path saved = directory.resolve("saved-group");
      Files.move(file, saved);
      Files.createDirectory(file);
      Path blocker = Files.writeString(file.resolve("blocker"), "test");
      assertThrows(java.io.IOException.class, () -> buffer.acknowledge(2));
      assertEquals(2, buffer.snapshot().deliveredSequence());
      assertEquals(charged, buffer.snapshot().bytes());
      Files.delete(blocker);
      Files.delete(file);
      Files.move(saved, file);
      buffer.acknowledge(2);
      assertEquals(0, buffer.snapshot().records());
      assertEquals(0, buffer.snapshot().bytes());
    }
    try (var buffer = new DiskBuffer(spool, limits)) {
      assertEquals(3, buffer.append("a:next", new byte[] {3}, Map.of()).sequence());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "checksum",
        "version",
        "count",
        "length",
        "frame_crc",
        "sequence",
        "truncated",
        "trailing"
      })
  void malformedGroupsFailClosedBeforeAdmissionReopens(String fault) throws Exception {
    Path file;
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.appendBatch(List.of(intent("a:one"), intent("a:two")));
      file = group(directory);
    }
    byte[] content = Files.readAllBytes(file);
    var data = ByteBuffer.wrap(content);
    boolean recheck = true;
    switch (fault) {
      case "checksum" -> {
        content[content.length - 1] ^= 1;
        recheck = false;
      }
      case "version" -> data.putInt(4, 99);
      case "count" -> data.putInt(8, 257);
      case "length" -> data.putInt(16, Integer.MAX_VALUE);
      case "frame_crc" -> content[content.length - 1] ^= 1;
      case "sequence" -> data.putLong(26, 999);
      case "truncated" -> {
        content = Arrays.copyOf(content, 12);
        recheck = false;
      }
      case "trailing" -> content = Arrays.copyOf(content, content.length + 1);
      default -> throw new AssertionError(fault);
    }
    if (recheck) {
      var crc = new CRC32C();
      crc.update(content, 16, content.length - 16);
      ByteBuffer.wrap(content).putInt(12, (int) crc.getValue());
    }
    Files.write(file, content);
    assertThrows(DiskBuffer.Corrupt.class, () -> new DiskBuffer(directory, limits));
  }

  @Test
  void overlappingLegacyAndGroupSequencesAreRejectedRegardlessOfDirectoryOrder() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.appendBatch(List.of(intent("a:one"), intent("a:two")));
    }
    byte[] content = Files.readAllBytes(group(directory));
    int length = ByteBuffer.wrap(content).getInt(16);
    Files.write(
        directory.resolve("00000000000000000001.evt"),
        Arrays.copyOfRange(content, 20, 20 + length));
    assertThrows(DiskBuffer.Corrupt.class, () -> new DiskBuffer(directory, limits));
  }

  @Test
  void filenameRangeMustMatchTheContainedSequenceRange() throws Exception {
    try (var buffer = new DiskBuffer(directory, limits)) {
      buffer.appendBatch(List.of(intent("a:one"), intent("a:two")));
    }
    Files.move(
        group(directory), directory.resolve("00000000000000000002-00000000000000000003.grp"));
    assertThrows(DiskBuffer.Corrupt.class, () -> new DiskBuffer(directory, limits));
  }

  @Test
  void interruptedTemporaryGroupsAreDiscardedWithoutSkippingASequence() throws Exception {
    Files.write(
        directory.resolve("00000000000000000001-00000000000000000002.tmp"), new byte[] {1, 2, 3});
    try (var buffer = new DiskBuffer(directory, limits)) {
      assertEquals(1, buffer.append("a:first", new byte[] {1}, Map.of()).sequence());
    }
  }

  @Test
  void completedGroupSurvivesForcedProcessTermination() throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    Process child =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                GroupCrashProbe.class.getName(),
                directory.toString())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    try {
      var reader =
          new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
      assertEquals(
          "COMMITTED 3", assertTimeoutPreemptively(Duration.ofSeconds(10), reader::readLine));
      child.destroyForcibly();
      assertTrue(child.waitFor(5, TimeUnit.SECONDS));
      try (var buffer = new DiskBuffer(directory, limits)) {
        assertEquals(
            List.of("a:one", "a:two", "a:three"),
            buffer.peekBatch(10, 4096).stream().map(DiskBuffer.Entry::key).toList());
      }
    } finally {
      child.destroyForcibly();
      child.waitFor(5, TimeUnit.SECONDS);
    }
  }
}
