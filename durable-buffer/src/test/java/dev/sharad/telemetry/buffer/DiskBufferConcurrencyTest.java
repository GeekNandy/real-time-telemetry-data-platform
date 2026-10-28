package dev.sharad.telemetry.buffer;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.config.PlatformSettings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskBufferConcurrencyTest {
  @TempDir Path directory;
  private final PlatformSettings.Buffer limits = new PlatformSettings.Buffer(1_048_576, 128, 4096);

  @Test
  void aSlowCheckpointDoesNotHoldAppendOrObservationBehindItsFsync() throws Exception {
    var disk = new PausingDisk("checkpoint.tmp");
    try (var buffer = new DiskBuffer(directory, limits, disk);
        var pool = Executors.newFixedThreadPool(2)) {
      var first = buffer.append("tenant:first", bytes("first"), Map.of());
      disk.armed.set(true);
      var checkpoint =
          pool.submit(
              () -> {
                buffer.acknowledge(first.sequence());
                return null;
              });
      try {
        assertTrue(disk.entered.await(5, TimeUnit.SECONDS));
        var second = pool.submit(() -> buffer.append("tenant:second", bytes("second"), Map.of()));
        assertEquals(2, second.get(5, TimeUnit.SECONDS).sequence());
        var snapshot = pool.submit(buffer::snapshot).get(5, TimeUnit.SECONDS);
        assertEquals(2, snapshot.records());
        assertEquals(
            0, snapshot.deliveredSequence(), "An unfinished fsync cannot advance delivery");
        assertFalse(checkpoint.isDone());
      } finally {
        disk.release.countDown();
      }
      checkpoint.get(5, TimeUnit.SECONDS);
      assertEquals(1, buffer.snapshot().records());
      assertEquals(2, buffer.peek().orElseThrow().sequence());
    }
    try (var recovered = new DiskBuffer(directory, limits)) {
      assertEquals(1, recovered.snapshot().records());
      assertEquals(2, recovered.peek().orElseThrow().sequence());
    }
  }

  @Test
  void aSlowAppendAllowsCheckpointingButCloseRetainsVolumeOwnershipUntilIoFinishes()
      throws Exception {
    var disk = new PausingDisk("00000000000000000002.tmp");
    var buffer = new DiskBuffer(directory, limits, disk);
    try (var pool = Executors.newFixedThreadPool(3)) {
      var first = buffer.append("tenant:first", bytes("first"), Map.of());
      disk.armed.set(true);
      var appending = pool.submit(() -> buffer.append("tenant:second", bytes("second"), Map.of()));
      try {
        assertTrue(disk.entered.await(5, TimeUnit.SECONDS));
        pool.submit(
                () -> {
                  buffer.acknowledge(first.sequence());
                  return null;
                })
            .get(5, TimeUnit.SECONDS);
        assertEquals(0, pool.submit(buffer::snapshot).get(5, TimeUnit.SECONDS).records());
        assertFalse(
            appending.isDone(), "The incomplete append must not be exposed for publication");
        var closing =
            pool.submit(
                () -> {
                  buffer.close();
                  return null;
                });
        assertThrows(IOException.class, () -> new DiskBuffer(directory, limits));
        disk.release.countDown();
        assertEquals(2, appending.get(5, TimeUnit.SECONDS).sequence());
        closing.get(5, TimeUnit.SECONDS);
      } finally {
        disk.release.countDown();
      }
    } finally {
      buffer.close();
    }
    try (var recovered = new DiskBuffer(directory, limits)) {
      assertArrayEquals(bytes("second"), recovered.peek().orElseThrow().payload());
      assertEquals(2, recovered.peek().orElseThrow().sequence());
    }
  }

  @Test
  void failedAppendDirectorySyncKeepsItsAmbiguousRecordChargedAndReplayable() throws Exception {
    var fail = new AtomicBoolean();
    var disk =
        new DiskBuffer.Durability() {
          @Override
          public void writeForced(Path path, byte[] content) throws IOException {
            DiskBuffer.FILESYSTEM.writeForced(path, content);
          }

          @Override
          public void forceDirectory(Path path) throws IOException {
            if (fail.getAndSet(false)) throw new IOException("Injected directory sync failure");
            DiskBuffer.FILESYSTEM.forceDirectory(path);
          }
        };
    var oneRecord = new PlatformSettings.Buffer(4096, 1, 1024);
    try (var buffer = new DiskBuffer(directory, oneRecord, disk)) {
      fail.set(true);
      assertThrows(
          IOException.class, () -> buffer.append("tenant:ambiguous", bytes("one"), Map.of()));
      assertEquals(1, buffer.snapshot().records());
      assertTrue(buffer.snapshot().bytes() > 0);
      assertThrows(
          DiskBuffer.Full.class, () -> buffer.append("tenant:excess", bytes("two"), Map.of()));
    }
    try (var recovered = new DiskBuffer(directory, oneRecord)) {
      assertEquals(1, recovered.peek().orElseThrow().sequence());
      recovered.acknowledge(1);
      assertEquals(2, recovered.append("tenant:next", bytes("two"), Map.of()).sequence());
    }
  }

  private static final class PausingDisk implements DiskBuffer.Durability {
    final AtomicBoolean armed = new AtomicBoolean();
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final String filename;

    PausingDisk(String filename) {
      this.filename = filename;
    }

    @Override
    public void writeForced(Path path, byte[] content) throws IOException {
      DiskBuffer.FILESYSTEM.writeForced(path, content);
      if (path.getFileName().toString().equals(filename) && armed.compareAndSet(true, false)) {
        entered.countDown();
        try {
          if (!release.await(10, TimeUnit.SECONDS)) throw new IOException("Test disk wait expired");
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IOException("Disk wait interrupted", interrupted);
        }
      }
    }

    @Override
    public void forceDirectory(Path path) throws IOException {
      DiskBuffer.FILESYSTEM.forceDirectory(path);
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
