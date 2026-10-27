package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.config.PlatformSettings;
import dev.sharad.telemetry.core.DurableLog;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaAcceptanceTest {
  @TempDir Path directory;
  private static final DurableLog.Position POSITION = new DurableLog.Position("events", 1, 42);

  private DiskBuffer buffer() throws IOException {
    return new DiskBuffer(directory, new PlatformSettings.Buffer(1_048_576, 128, 1024));
  }

  @Test
  void requiresPublicationAndDurableCheckpointBeforeCompleting() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 2, Duration.ofSeconds(5))) {
      var result =
          acceptance.append("tenant:event", new byte[] {1}, Map.of()).toCompletableFuture();
      var entry = buffer.peek().orElseThrow();
      assertFalse(result.isDone());
      assertEquals(1, buffer.snapshot().records());
      acceptance.confirm(entry.sequence(), POSITION);
      assertEquals(POSITION, result.get(1, TimeUnit.SECONDS));
      assertEquals(entry.sequence(), buffer.snapshot().deliveredSequence());
      assertEquals(0, buffer.snapshot().records());
      assertEquals(0, acceptance.pendingWaiters());
      assertEquals(0, acceptance.scheduledWaiters());
    }
  }

  @Test
  void timeoutReleasesOnlyTheWaiterAndAllowsLatePublication() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 1, Duration.ofMillis(80))) {
      var result =
          acceptance.append("tenant:first", new byte[] {1}, Map.of()).toCompletableFuture();
      var error = assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
      assertInstanceOf(TimeoutException.class, error.getCause());
      assertEquals(0, acceptance.pendingWaiters());
      assertEquals(1, buffer.snapshot().records());
      var second =
          acceptance.append("tenant:second", new byte[] {2}, Map.of()).toCompletableFuture();
      acceptance.confirm(buffer.peek().orElseThrow().sequence(), POSITION);
      acceptance.confirm(buffer.peek().orElseThrow().sequence(), POSITION);
      assertEquals(POSITION, second.get(1, TimeUnit.SECONDS));
      assertTrue(result.isCompletedExceptionally());
      assertEquals(0, buffer.snapshot().records());
    }
  }

  @Test
  void cancellationCannotCancelPublicationOrRemoveTheSpoolEntry() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 1, Duration.ofSeconds(5))) {
      var stage = acceptance.append("tenant:event", new byte[] {1}, Map.of());
      assertTrue(stage.toCompletableFuture().cancel(true));
      assertEquals(1, acceptance.pendingWaiters());
      assertEquals(1, buffer.snapshot().records());
      acceptance.confirm(buffer.peek().orElseThrow().sequence(), POSITION);
      assertEquals(POSITION, stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void rejectsBeyondTheWaiterBudgetWithoutAppendingAnotherRecord() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 1, Duration.ofSeconds(5))) {
      acceptance.append("tenant:first", new byte[] {1}, Map.of());
      assertThrows(
          RejectedExecutionException.class,
          () -> acceptance.append("tenant:excess", new byte[] {2}, Map.of()));
      assertEquals(1, acceptance.pendingWaiters());
      assertEquals(1, acceptance.scheduledWaiters());
      assertEquals(1, buffer.snapshot().records());
    }
  }

  @Test
  void checkpointFailureCannotInventSuccessfulAcceptance() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 1, Duration.ofSeconds(5))) {
      var result =
          acceptance.append("tenant:event", new byte[] {1}, Map.of()).toCompletableFuture();
      long sequence = buffer.peek().orElseThrow().sequence();
      Files.createDirectory(directory.resolve("checkpoint"));
      assertThrows(IOException.class, () -> acceptance.confirm(sequence, POSITION));
      assertFalse(result.isDone());
      assertEquals(1, buffer.snapshot().records());
      Files.delete(directory.resolve("checkpoint"));
      acceptance.confirm(sequence, POSITION);
      assertEquals(POSITION, result.get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void recoveredRecordsNeedNoLiveHttpWaiter() throws Exception {
    try (var buffer = buffer()) {
      buffer.append("tenant:recovery", new byte[] {1}, Map.of());
    }
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 1, Duration.ofSeconds(5))) {
      acceptance.confirm(buffer.peek().orElseThrow().sequence(), POSITION);
      assertEquals(0, buffer.snapshot().records());
      assertEquals(0, acceptance.pendingWaiters());
    }
  }

  @Test
  void closingSettlesWaitersButRetainsDurableWorkForRestart() throws Exception {
    try (var buffer = buffer()) {
      var acceptance = new KafkaAcceptance(buffer, 1, Duration.ofSeconds(5));
      var result =
          acceptance.append("tenant:event", new byte[] {1}, Map.of()).toCompletableFuture();
      acceptance.close();
      assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
      assertEquals(1, buffer.snapshot().records());
      assertEquals(0, acceptance.pendingWaiters());
      assertEquals(0, acceptance.scheduledWaiters());
      assertThrows(
          IOException.class, () -> acceptance.append("tenant:closed", new byte[] {2}, Map.of()));
    }
  }

  @Test
  void fastDispatcherCannotPublishBetweenAppendAndWaiterRegistration() throws Exception {
    try (var buffer = buffer();
        var acceptance = new KafkaAcceptance(buffer, 128, Duration.ofSeconds(5));
        var publisher = Executors.newSingleThreadExecutor();
        var appenders = Executors.newFixedThreadPool(4)) {
      var publishing =
          publisher.submit(
              () -> {
                for (int count = 0; count < 80; count++) {
                  await().atMost(Duration.ofSeconds(5)).until(() -> buffer.peek().isPresent());
                  acceptance.confirm(buffer.peek().orElseThrow().sequence(), POSITION);
                }
                return null;
              });
      var results = new java.util.ArrayList<java.util.concurrent.Future<DurableLog.Position>>();
      for (int count = 0; count < 80; count++) {
        int id = count;
        results.add(
            appenders.submit(
                () ->
                    acceptance
                        .append("tenant:" + id, new byte[] {1}, Map.of())
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)));
      }
      for (var result : results) assertEquals(POSITION, result.get(10, TimeUnit.SECONDS));
      publishing.get(5, TimeUnit.SECONDS);
      assertEquals(0, acceptance.pendingWaiters());
      assertEquals(0, acceptance.scheduledWaiters());
    }
  }
}
