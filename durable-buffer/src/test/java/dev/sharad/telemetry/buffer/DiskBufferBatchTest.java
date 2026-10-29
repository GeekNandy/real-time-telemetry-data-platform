package dev.sharad.telemetry.buffer;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.config.PlatformSettings;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskBufferBatchTest {
  @TempDir Path directory;
  private final PlatformSettings.Buffer limits = new PlatformSettings.Buffer(1_048_576, 128, 4096);

  private static DiskBuffer.Append intent(String key) {
    return new DiskBuffer.Append(key, new byte[] {1, 2, 3}, Map.of("traceparent", "test-context"));
  }

  @Test
  void everyFileIsForcedBeforeTheSharedDirectoryBarrierAndUsesTheExistingRecoveryFormat()
      throws Exception {
    var files = new AtomicInteger();
    var directories = new AtomicInteger();
    var disk =
        new DiskBuffer.Durability() {
          public void writeForced(Path path, byte[] content) throws IOException {
            DiskBuffer.FILESYSTEM.writeForced(path, content);
            files.incrementAndGet();
          }

          public void forceDirectory(Path path) throws IOException {
            DiskBuffer.FILESYSTEM.forceDirectory(path);
            directories.incrementAndGet();
          }
        };
    try (var buffer = new DiskBuffer(directory, limits, disk)) {
      directories.set(0);
      var result = buffer.appendBatch(List.of(intent("a:one"), intent("a:two"), intent("a:three")));
      assertEquals(List.of(1L, 2L, 3L), result.stream().map(DiskBuffer.Entry::sequence).toList());
      assertEquals(3, files.get());
      assertEquals(1, directories.get());
      assertEquals(3, buffer.snapshot().records());
      buffer.acknowledgeBatch(List.of(1L, 2L));
    }
    try (var recovered = new DiskBuffer(directory, limits)) {
      assertEquals(1, recovered.snapshot().records());
      assertEquals("a:three", recovered.peek().orElseThrow().key());
      assertEquals(
          "test-context", recovered.peek().orElseThrow().traceHeaders().get("traceparent"));
      assertEquals(4, recovered.append("a:four", new byte[] {4}, Map.of()).sequence());
    }
  }

  @Test
  void validatesEveryFrameAndTheEntireBudgetBeforeWritingAnyMember() throws Exception {
    var twoRecords = new PlatformSettings.Buffer(4096, 2, 1024);
    try (var buffer = new DiskBuffer(directory.resolve("records"), twoRecords)) {
      assertThrows(
          DiskBuffer.Full.class,
          () -> buffer.appendBatch(List.of(intent("a:one"), intent("a:two"), intent("a:three"))));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              buffer.appendBatch(
                  List.of(
                      intent("a:valid"),
                      new DiskBuffer.Append("a:large", new byte[1024], Map.of()))));
      assertThrows(IllegalArgumentException.class, () -> buffer.appendBatch(List.of()));
      assertThrows(
          IllegalArgumentException.class,
          () -> buffer.appendBatch(java.util.Collections.nCopies(257, intent("a:too-many"))));
      assertEquals(0, buffer.snapshot().records());
      assertEquals(1, buffer.append("a:first", new byte[] {1}, Map.of()).sequence());
    }
    var byteLimited = new PlatformSettings.Buffer(256, 100, 256);
    try (var buffer = new DiskBuffer(directory.resolve("bytes"), byteLimited)) {
      var large = new DiskBuffer.Append("a:item", new byte[100], Map.of());
      assertThrows(DiskBuffer.Full.class, () -> buffer.appendBatch(List.of(large, large)));
      assertEquals(0, buffer.snapshot().bytes());
    }
  }

  @Test
  void aPartialWriteFailureChargesAndRecoversItsRenamedPrefixWithoutSkippingASequence()
      throws Exception {
    var fail = new AtomicBoolean(true);
    var disk =
        new DiskBuffer.Durability() {
          public void writeForced(Path path, byte[] content) throws IOException {
            if (path.getFileName().toString().equals("00000000000000000002.tmp")
                && fail.getAndSet(false)) throw new IOException("Injected second-file failure");
            DiskBuffer.FILESYSTEM.writeForced(path, content);
          }

          public void forceDirectory(Path path) throws IOException {
            DiskBuffer.FILESYSTEM.forceDirectory(path);
          }
        };
    try (var buffer = new DiskBuffer(directory, limits, disk)) {
      assertThrows(
          IOException.class,
          () -> buffer.appendBatch(List.of(intent("a:one"), intent("a:two"), intent("a:three"))));
      assertEquals(1, buffer.snapshot().records());
      assertEquals("a:one", buffer.peek().orElseThrow().key());
      assertEquals(2, buffer.append("a:retry", new byte[] {2}, Map.of()).sequence());
    }
    try (var recovered = new DiskBuffer(directory, limits)) {
      assertEquals(
          List.of("a:one", "a:retry"),
          recovered.peekBatch(10, 4096).stream().map(DiskBuffer.Entry::key).toList());
    }
  }

  @Test
  void boundsMultiRecordBytesWithoutBreakingTheConfiguredSingleRecordAppendLimit()
      throws Exception {
    int mib = 1024 * 1024;
    var largeLimits = new PlatformSettings.Buffer(12L * mib, 10, 5 * mib);
    try (var buffer = new DiskBuffer(directory, largeLimits)) {
      var threeMib = new DiskBuffer.Append("a:large", new byte[3 * mib], Map.of());
      assertThrows(
          IllegalArgumentException.class, () -> buffer.appendBatch(List.of(threeMib, threeMib)));
      assertEquals(0, buffer.snapshot().records());
      assertEquals(1, buffer.append("a:single", new byte[4 * mib + 128], Map.of()).sequence());
    }
    try (var recovered = new DiskBuffer(directory, largeLimits)) {
      assertEquals(4 * mib + 128, recovered.peek().orElseThrow().payload().length);
    }
  }

  @Test
  void anAmbiguousDirectoryFailureRetainsEveryRenamedMemberForReplay() throws Exception {
    var fail = new AtomicBoolean();
    var disk =
        new DiskBuffer.Durability() {
          public void writeForced(Path path, byte[] content) throws IOException {
            DiskBuffer.FILESYSTEM.writeForced(path, content);
          }

          public void forceDirectory(Path path) throws IOException {
            if (fail.getAndSet(false)) throw new IOException("Injected directory sync failure");
            DiskBuffer.FILESYSTEM.forceDirectory(path);
          }
        };
    try (var buffer = new DiskBuffer(directory, limits, disk)) {
      fail.set(true);
      assertThrows(
          IOException.class, () -> buffer.appendBatch(List.of(intent("a:one"), intent("a:two"))));
      assertEquals(2, buffer.snapshot().records());
    }
    try (var recovered = new DiskBuffer(directory, limits)) {
      assertEquals(2, recovered.peekBatch(10, 4096).size());
      recovered.acknowledgeBatch(List.of(1L, 2L));
      assertEquals(3, recovered.append("a:next", new byte[] {3}, Map.of()).sequence());
    }
  }

  @Test
  void noMemberIsVisibleWhileTheDirectoryDurabilityBarrierIsStillBlocked() throws Exception {
    var armed = new AtomicBoolean();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var disk =
        new DiskBuffer.Durability() {
          public void writeForced(Path path, byte[] content) throws IOException {
            DiskBuffer.FILESYSTEM.writeForced(path, content);
          }

          public void forceDirectory(Path path) throws IOException {
            if (armed.getAndSet(false)) {
              entered.countDown();
              try {
                if (!release.await(10, TimeUnit.SECONDS))
                  throw new IOException("Disk wait expired");
              } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IOException(error);
              }
            }
            DiskBuffer.FILESYSTEM.forceDirectory(path);
          }
        };
    try (var buffer = new DiskBuffer(directory, limits, disk);
        var pool = Executors.newFixedThreadPool(2)) {
      armed.set(true);
      var appending =
          pool.submit(() -> buffer.appendBatch(List.of(intent("a:one"), intent("a:two"))));
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(pool.submit(buffer::peek).get(5, TimeUnit.SECONDS).isEmpty());
        assertEquals(0, buffer.snapshot().records());
        assertFalse(appending.isDone());
      } finally {
        release.countDown();
      }
      assertEquals(2, appending.get(5, TimeUnit.SECONDS).size());
    }
  }

  @Test
  void publicationCannotOvertakePostSyncWaiterRegistration() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var buffer = new DiskBuffer(directory, limits);
        var pool = Executors.newFixedThreadPool(2)) {
      var appending =
          pool.submit(
              () ->
                  buffer.appendBatch(
                      List.of(intent("a:one")),
                      entries -> {
                        assertEquals(1, entries.getFirst().sequence());
                        entered.countDown();
                        try {
                          if (!release.await(10, TimeUnit.SECONDS))
                            throw new IllegalStateException("Registration wait expired");
                        } catch (InterruptedException error) {
                          Thread.currentThread().interrupt();
                          throw new IllegalStateException(error);
                        }
                      }));
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(pool.submit(buffer::peek).get(5, TimeUnit.SECONDS).isEmpty());
        assertFalse(appending.isDone());
      } finally {
        release.countDown();
      }
      assertEquals(1, appending.get(5, TimeUnit.SECONDS).size());
      assertEquals(1, buffer.peek().orElseThrow().sequence());
    }
  }
}
