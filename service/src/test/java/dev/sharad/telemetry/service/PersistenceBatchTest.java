package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.DefaultErrorHandler;

class PersistenceBatchTest {
  private static final class ClosingMeters extends SimpleMeterRegistry implements AutoCloseable {}

  private static Event event(int index) {
    return new Event(
        new UUID(0, index + 1),
        "batch-fixture",
        "performance.sample",
        1,
        Instant.EPOCH,
        Map.of("value", "1"));
  }

  private static List<ConsumerRecord<String, byte[]>> records(int count) {
    var rows = new ArrayList<ConsumerRecord<String, byte[]>>();
    for (int i = 0; i < count; i++)
      rows.add(
          new ConsumerRecord<>("events", i % 3, i, event(i).partitionKey(), new byte[] {(byte) i}));
    return rows;
  }

  private static Function<byte[], Event> decoder() {
    return value -> event(Byte.toUnsignedInt(value[0]));
  }

  private static PersistenceConsumer consumer(
      EventSink sink, Function<byte[], Event> decode, SimpleMeterRegistry meters) {
    return new PersistenceConsumer(
        sink, decode, new PlatformTelemetry(OpenTelemetry.noop(), meters), meters);
  }

  @Test
  void commitsOnceOnlyAfterEverySequentialTransactionFinishes() {
    try (var meters = new ClosingMeters()) {
      var completed = new ArrayList<UUID>();
      var acknowledgements = new AtomicInteger();
      var consumer =
          consumer(
              e -> {
                completed.add(e.eventId());
                return EventSink.Outcome.INSERTED;
              },
              decoder(),
              meters);
      consumer.consume(
          records(5),
          () -> {
            assertEquals(5, completed.size());
            acknowledgements.incrementAndGet();
          });
      assertEquals(1, acknowledgements.get());
      assertEquals(5, meters.get("telemetry.persistence.batch.size").summary().totalAmount());
      assertEquals(1, meters.get("telemetry.persistence.batch.commit").timer().count());
    }
  }

  @Test
  void aDatabaseFailureIdentifiesOnlyTheDurablePrefixAndStopsLaterRecords() {
    try (var meters = new ClosingMeters()) {
      var stored = new ArrayList<UUID>();
      var calls = new AtomicInteger();
      var outage =
          new org.springframework.dao.DataAccessResourceFailureException("Synthetic outage");
      var consumer =
          consumer(
              e -> {
                if (calls.getAndIncrement() == 2) throw outage;
                stored.add(e.eventId());
                return EventSink.Outcome.INSERTED;
              },
              decoder(),
              meters);
      var failed =
          assertThrows(
              BatchListenerFailedException.class,
              () -> consumer.consume(records(5), () -> fail("Failed batch committed")));
      assertEquals(2, failed.getIndex());
      assertSame(outage, failed.getCause());
      assertEquals(List.of(event(0).eventId(), event(1).eventId()), stored);
      assertEquals(3, calls.get());
    }
  }

  @Test
  void poisonAtAMiddleIndexCannotCommitOrProcessTheSuffix() {
    try (var meters = new ClosingMeters()) {
      var stored = new ArrayList<UUID>();
      var rows = records(3);
      rows.set(1, new ConsumerRecord<>("events", 1, 1, "wrong-tenant-key", new byte[] {1}));
      var consumer =
          consumer(
              e -> {
                stored.add(e.eventId());
                return EventSink.Outcome.INSERTED;
              },
              decoder(),
              meters);
      var failed =
          assertThrows(
              BatchListenerFailedException.class,
              () -> consumer.consume(rows, () -> fail("Poison batch committed")));
      assertEquals(1, failed.getIndex());
      assertInstanceOf(PersistenceConsumer.PoisonRecord.class, failed.getCause());
      assertEquals(List.of(event(0).eventId()), stored);
    }
  }

  @Test
  void aCommitFailureReplaysACompletelyStoredBatchRatherThanQuarantiningAnEvent() {
    try (var meters = new ClosingMeters()) {
      var stored = new java.util.HashSet<UUID>();
      var ack = new AtomicInteger();
      var consumer =
          consumer(
              e ->
                  stored.add(e.eventId())
                      ? EventSink.Outcome.INSERTED
                      : EventSink.Outcome.DUPLICATE,
              decoder(),
              meters);
      var outage = new IllegalStateException("Synthetic commit outage");
      assertSame(
          outage,
          assertThrows(
              IllegalStateException.class,
              () ->
                  consumer.consume(
                      records(5),
                      () -> {
                        throw outage;
                      })));
      consumer.consume(records(5), ack::incrementAndGet);
      assertEquals(5, stored.size());
      assertEquals(1, ack.get());
    }
  }

  @Test
  void registryUnavailabilityRemainsRetryableAndKeepsItsExactFailureIndex() {
    try (var meters = new ClosingMeters()) {
      var outage = new IllegalStateException("Synthetic registry outage");
      var consumer =
          consumer(
              e -> {
                fail("Unknown schema reached storage");
                return EventSink.Outcome.INSERTED;
              },
              bytes -> {
                throw outage;
              },
              meters);
      var failed =
          assertThrows(
              BatchListenerFailedException.class,
              () -> consumer.consume(records(3), () -> fail("Registry outage committed")));
      assertEquals(0, failed.getIndex());
      assertSame(outage, failed.getCause());
    }
  }

  @Test
  void excessiveAndEmptyBatchesCannotReachStorageOrCommit() {
    try (var meters = new ClosingMeters()) {
      var consumer =
          consumer(
              e -> {
                fail("Unexpected storage");
                return EventSink.Outcome.INSERTED;
              },
              decoder(),
              meters);
      consumer.consume(List.of(), () -> fail("Empty batch committed"));
      var failed =
          assertThrows(
              BatchListenerFailedException.class,
              () -> consumer.consume(records(257), () -> fail("Oversized batch committed")));
      assertEquals(0, failed.getIndex());
    }
  }

  @Test
  void startupRejectsUnboundedPollConfiguration() {
    for (int count : new int[] {0, 257, 500}) {
      var factory =
          new DefaultKafkaConsumerFactory<String, byte[]>(Map.of("max.poll.records", count));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ConsumerConfiguration()
                  .kafkaListenerContainerFactory(factory, new DefaultErrorHandler()));
    }
  }
}
