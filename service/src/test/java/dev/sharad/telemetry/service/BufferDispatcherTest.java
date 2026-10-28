package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.config.PlatformSettings;
import dev.sharad.telemetry.core.DurableLog;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BufferDispatcherTest {
  @TempDir Path directory;

  private final class Fixture implements AutoCloseable {
    final DiskBuffer buffer;
    final KafkaAcceptance acceptance;
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    BufferDispatcher.Publisher publisher =
        record ->
            CompletableFuture.failedFuture(new IllegalStateException("Unconfigured publisher"));
    final List<ProducerRecord<String, byte[]>> sent = new ArrayList<>();
    final BufferDispatcher dispatcher;

    Fixture(int batchRecords, Duration publicationDeadline) throws IOException {
      buffer = new DiskBuffer(directory, new PlatformSettings.Buffer(1_048_576, 128, 1024));
      acceptance = new KafkaAcceptance(buffer, 128, Duration.ofSeconds(30));
      dispatcher =
          new BufferDispatcher(
              buffer,
              acceptance,
              record -> {
                sent.add(record);
                return publisher.send(record);
              },
              new PlatformTelemetry(OpenTelemetry.noop(), meters),
              "events",
              meters,
              batchRecords,
              1_048_576,
              publicationDeadline);
    }

    List<CompletableFuture<DurableLog.Position>> append(int count) throws IOException {
      var results = new ArrayList<CompletableFuture<DurableLog.Position>>();
      for (int id = 0; id < count; id++)
        results.add(
            acceptance
                .append("tenant:" + id, new byte[] {(byte) id}, Map.of())
                .toCompletableFuture());
      return results;
    }

    @Override
    public void close() throws IOException {
      dispatcher.stop();
      acceptance.close();
      buffer.close();
      meters.close();
    }
  }

  private static DurableLog.Position result(long offset) {
    return new DurableLog.Position("events", 0, offset);
  }

  @Test
  void submitsTheWholeWindowBeforeWaitingAndCompletesAfterAllAcknowledgements() throws Exception {
    try (var f = new Fixture(3, Duration.ofSeconds(5))) {
      var receipts = f.append(3);
      var futures =
          List.of(
              new CompletableFuture<DurableLog.Position>(),
              new CompletableFuture<DurableLog.Position>(),
              new CompletableFuture<DurableLog.Position>());
      var calls = new AtomicInteger();
      f.publisher =
          record -> {
            int index = calls.getAndIncrement();
            if (index == 2) {
              futures.get(0).complete(result(10));
              futures.get(1).complete(result(11));
              assertTrue(receipts.stream().noneMatch(CompletableFuture::isDone));
              assertEquals(0, f.buffer.snapshot().deliveredSequence());
              futures.get(2).complete(result(12));
            }
            return futures.get(index);
          };
      assertEquals(3, f.dispatcher.dispatchOnce());
      assertEquals(List.of(10L, 11L, 12L), receipts.stream().map(r -> r.join().offset()).toList());
      assertEquals(0, f.buffer.snapshot().records());
      assertEquals(3, f.buffer.snapshot().deliveredSequence());
      assertEquals(3, f.meters.get("telemetry.buffer.delivered").counter().count());
      assertEquals(
          1,
          f.meters
              .get("telemetry.dispatch.phase.duration")
              .tag("phase", "checkpoint")
              .timer()
              .count());
    }
  }

  @Test
  void oneFailedAcknowledgementRetainsTheEntirePrefixForRetry() throws Exception {
    try (var f = new Fixture(3, Duration.ofSeconds(5))) {
      var receipts = f.append(3);
      var responses = new ArrayDeque<CompletableFuture<DurableLog.Position>>();
      responses.add(CompletableFuture.completedFuture(result(1)));
      responses.add(CompletableFuture.failedFuture(new IOException("broker unavailable")));
      responses.add(CompletableFuture.completedFuture(result(3)));
      f.publisher = record -> responses.remove();
      assertEquals(-1, f.dispatcher.dispatchOnce());
      assertEquals(3, f.buffer.snapshot().records());
      assertEquals(0, f.buffer.snapshot().deliveredSequence());
      assertTrue(receipts.stream().noneMatch(CompletableFuture::isDone));
      for (int id = 0; id < 3; id++)
        responses.add(CompletableFuture.completedFuture(result(10 + id)));
      assertEquals(3, f.dispatcher.dispatchOnce());
      assertTrue(receipts.stream().allMatch(CompletableFuture::isDone));
      assertEquals(0, f.buffer.snapshot().records());
    }
  }

  @Test
  void producerAcknowledgementsDoNotBypassAFailedCheckpoint() throws Exception {
    try (var f = new Fixture(3, Duration.ofSeconds(5))) {
      var receipts = f.append(3);
      f.publisher = record -> CompletableFuture.completedFuture(result(1));
      Files.createDirectory(directory.resolve("checkpoint"));
      assertEquals(-1, f.dispatcher.dispatchOnce());
      assertTrue(receipts.stream().noneMatch(CompletableFuture::isDone));
      assertEquals(0, f.buffer.snapshot().deliveredSequence());
      Files.delete(directory.resolve("checkpoint"));
      assertEquals(3, f.dispatcher.dispatchOnce());
      assertTrue(receipts.stream().allMatch(CompletableFuture::isDone));
      assertEquals(0, f.buffer.snapshot().records());
    }
  }

  @Test
  void configuredWindowLeavesTheTailPending() throws Exception {
    try (var f = new Fixture(2, Duration.ofSeconds(5))) {
      var receipts = f.append(5);
      f.publisher = record -> CompletableFuture.completedFuture(result(1));
      assertEquals(2, f.dispatcher.dispatchOnce());
      assertEquals(3, f.buffer.snapshot().records());
      assertTrue(receipts.get(1).isDone());
      assertFalse(receipts.get(2).isDone());
      assertEquals(2, f.dispatcher.dispatchOnce());
      assertEquals(1, f.dispatcher.dispatchOnce());
      assertEquals(5, f.sent.size());
    }
  }

  @Test
  void timeoutAndClientCancellationCannotCancelPublicationOrLoseThePrefix() throws Exception {
    try (var f = new Fixture(2, Duration.ofMillis(80))) {
      var receipts = f.append(2);
      receipts.getFirst().cancel(true);
      var late = new CompletableFuture<DurableLog.Position>();
      var calls = new AtomicInteger();
      f.publisher =
          record ->
              calls.getAndIncrement() == 0 ? CompletableFuture.completedFuture(result(1)) : late;
      assertEquals(-1, f.dispatcher.dispatchOnce());
      assertFalse(late.isCancelled());
      assertFalse(receipts.get(1).isDone());
      assertEquals(2, f.buffer.snapshot().records());
      assertEquals(0, f.buffer.snapshot().deliveredSequence());
      late.complete(result(2));
      f.publisher = record -> CompletableFuture.completedFuture(result(3));
      assertEquals(2, f.dispatcher.dispatchOnce());
      assertEquals(0, f.buffer.snapshot().records());
      assertEquals(3, receipts.get(1).join().offset());
    }
  }
}
