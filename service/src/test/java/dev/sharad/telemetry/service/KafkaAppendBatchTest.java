package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.config.PlatformSettings;
import dev.sharad.telemetry.core.DurableLog;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaAppendBatchTest {
  @TempDir Path directory;
  private static final DurableLog.Position POSITION = new DurableLog.Position("events", 0, 1);
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

  @AfterEach
  void closeMeters() {
    meters.close();
  }

  private DiskBuffer buffer() throws IOException {
    return new DiskBuffer(directory, new PlatformSettings.Buffer(1_048_576, 128, 1024));
  }

  private static void waitFor(CountDownLatch latch) throws IOException {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) throw new IOException("Test append wait expired");
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException(interrupted);
    }
  }

  @Test
  void queuedArrivalsShareBoundedAppendsButStillWaitForKafkaAndTheCheckpoint() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var first = new AtomicBoolean(true);
    List<Integer> sizes = Collections.synchronizedList(new ArrayList<>());
    try (var buffer = buffer();
        var acceptance =
            new KafkaAcceptance(
                buffer,
                64,
                Duration.ofSeconds(30),
                meters,
                records -> {
                  sizes.add(records.size());
                  if (first.getAndSet(false)) {
                    entered.countDown();
                    waitFor(release);
                  }
                  return buffer.appendBatch(records);
                })) {
      var results = new ArrayList<CompletableFuture<DurableLog.Position>>();
      results.add(
          acceptance.append("tenant:first", new byte[] {1}, Map.of()).toCompletableFuture());
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        for (int id = 0; id < 32; id++)
          results.add(
              acceptance.append("tenant:" + id, new byte[] {1}, Map.of()).toCompletableFuture());
        assertEquals(33, acceptance.pendingWrites());
        assertTrue(results.stream().noneMatch(CompletableFuture::isDone));
      } finally {
        release.countDown();
      }
      await().atMost(Duration.ofSeconds(5)).until(() -> acceptance.pendingWrites() == 0);
      assertEquals(List.of(1, 32), sizes);
      assertEquals(33, buffer.snapshot().records());
      assertTrue(results.stream().noneMatch(CompletableFuture::isDone));
      acceptance.confirmBatch(
          buffer.peekBatch(64, 4096).stream()
              .map(e -> new KafkaAcceptance.Confirmed(e.sequence(), POSITION))
              .toList());
      for (var result : results) assertEquals(POSITION, result.get(5, TimeUnit.SECONDS));
      assertEquals(0, acceptance.pendingBytes());
      assertEquals(0, acceptance.pendingWaiters());
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertEquals(33, meters.get("telemetry.spool.append.duration").timer().count()));
    }
  }

  @Test
  void timedOutQueuedWorkRemainsChargedUntilItsDiskAttemptFinishes() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var buffer = buffer();
        var acceptance =
            new KafkaAcceptance(
                buffer,
                2,
                Duration.ofMillis(150),
                meters,
                records -> {
                  entered.countDown();
                  waitFor(release);
                  return buffer.appendBatch(records);
                })) {
      var first = acceptance.append("tenant:first", new byte[] {1}, Map.of()).toCompletableFuture();
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        var second =
            acceptance.append("tenant:second", new byte[] {2}, Map.of()).toCompletableFuture();
        assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS));
        assertEquals(0, acceptance.pendingWaiters());
        assertEquals(2, acceptance.pendingWrites());
        assertThrows(
            RejectedExecutionException.class,
            () -> acceptance.append("tenant:excess", new byte[] {3}, Map.of()));
      } finally {
        release.countDown();
      }
      await().atMost(Duration.ofSeconds(5)).until(() -> acceptance.pendingWrites() == 0);
      assertEquals(2, buffer.snapshot().records());
      acceptance.confirmBatch(
          List.of(
              new KafkaAcceptance.Confirmed(1, POSITION),
              new KafkaAcceptance.Confirmed(2, POSITION)));
      assertEquals(0, buffer.snapshot().records());
    }
  }

  @Test
  void closeRejectsUnwrittenIntentsAndPreservesPreviouslyDurableWork() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var buffer = buffer()) {
      buffer.append("tenant:durable", new byte[] {1}, Map.of());
      var acceptance =
          new KafkaAcceptance(
              buffer,
              4,
              Duration.ofSeconds(30),
              meters,
              records -> {
                entered.countDown();
                waitFor(release);
                return buffer.appendBatch(records);
              });
      try {
        var first =
            acceptance.append("tenant:in-flight", new byte[] {2}, Map.of()).toCompletableFuture();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        var second =
            acceptance.append("tenant:queued", new byte[] {3}, Map.of()).toCompletableFuture();
        acceptance.close();
        assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS));
        assertEquals(0, acceptance.pendingWrites());
        assertEquals(0, acceptance.pendingBytes());
        assertEquals("tenant:durable", buffer.peek().orElseThrow().key());
        assertEquals(1, buffer.snapshot().records());
      } finally {
        release.countDown();
        acceptance.close();
      }
    }
  }

  @Test
  void anOversizedFrameCannotEnterAndPoisonAnotherRequestsBatch() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 4, Duration.ofSeconds(30))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> acceptance.append("tenant:large", new byte[1020], Map.of()));
      var result =
          acceptance.append("tenant:valid", new byte[] {1}, Map.of()).toCompletableFuture();
      await().atMost(Duration.ofSeconds(5)).until(() -> acceptance.pendingWrites() == 0);
      acceptance.confirm(1, POSITION);
      assertEquals(POSITION, result.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void aFailedBatchCannotInventReceiptsForItsReplayableDurablePrefix() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var firstCall = new AtomicBoolean(true);
    try (var buffer = buffer();
        var acceptance =
            new KafkaAcceptance(
                buffer,
                8,
                Duration.ofSeconds(30),
                meters,
                records -> {
                  if (firstCall.getAndSet(false)) {
                    entered.countDown();
                    waitFor(release);
                  }
                  buffer.appendBatch(List.of(records.getFirst()));
                  throw new IOException("Injected partial append failure");
                })) {
      var first = acceptance.append("tenant:first", new byte[] {1}, Map.of()).toCompletableFuture();
      var results = new ArrayList<CompletableFuture<DurableLog.Position>>();
      results.add(first);
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        results.add(
            acceptance.append("tenant:second", new byte[] {2}, Map.of()).toCompletableFuture());
        results.add(
            acceptance.append("tenant:third", new byte[] {3}, Map.of()).toCompletableFuture());
      } finally {
        release.countDown();
      }
      for (var result : results)
        assertInstanceOf(
            IOException.class,
            assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS))
                .getCause());
      assertEquals(0, acceptance.pendingWrites());
      assertEquals(0, acceptance.pendingWaiters());
      assertEquals(2, buffer.snapshot().records());
      acceptance.confirmBatch(
          buffer.peekBatch(8, 4096).stream()
              .map(e -> new KafkaAcceptance.Confirmed(e.sequence(), POSITION))
              .toList());
      assertTrue(results.stream().allMatch(CompletableFuture::isCompletedExceptionally));
      assertEquals(0, buffer.snapshot().records());
    }
  }
}
