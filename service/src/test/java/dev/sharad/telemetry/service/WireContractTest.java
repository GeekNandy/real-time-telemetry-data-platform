package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import dev.sharad.telemetry.schema.SchemaCatalog;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

class WireContractTest {
  private final JsonMapper mapper = new RuntimeConfiguration().jsonMapper();

  @Test
  void roundTripsTheDatabaseJsonValues() {
    var event =
        new Event(
            UUID.randomUUID(),
            "tenant-a",
            "performance.sample",
            1,
            Instant.parse("2026-01-01T00:00:00.123456789Z"),
            Map.of("metric", "frame_ms", "value", "16"));
    assertEquals(event, mapper.readValue(mapper.writeValueAsString(event), Event.class));
  }

  @Test
  void refusesClientSuppliedTenant() {
    String json =
        """
        {"eventId":"00000000-0000-0000-0000-000000000001",
         "eventType":"session.started","schemaVersion":1,
         "occurredAt":"2026-01-01T00:00:00Z","attributes":{},"tenantId":"tenant-b"}
        """;
    assertThrows(RuntimeException.class, () -> mapper.readValue(json, EventRequest.class));
  }

  @Test
  void quarantinesInvalidBrokerPayloadBeforeTouchingStorage() {
    EventSink sink =
        event -> {
          throw new AssertionError("Invalid data reached persistence");
        };
    Acknowledgment ack =
        () -> {
          throw new AssertionError("Invalid data committed its offset");
        };
    var catalog =
        new SchemaCatalog() {
          public int registerOrLookup(String subject, Schema schema) {
            return 1;
          }

          public Schema byId(int id) {
            return dev.sharad.telemetry.schema.EventSchemas.v2();
          }
        };
    var meters = new SimpleMeterRegistry();
    try {
      var consumer =
          new PersistenceConsumer(
              sink,
              new AvroEventCodec(catalog),
              new PlatformTelemetry(OpenTelemetry.noop(), meters),
              meters);
      for (byte[] payload : new byte[][] {null, new byte[0], new byte[] {1, 2, 3}}) {
        var failed =
            assertThrows(
                org.springframework.kafka.listener.BatchListenerFailedException.class,
                () ->
                    consumer.consume(
                        java.util.List.of(new ConsumerRecord<>("events", 0, 0, "invalid", payload)),
                        ack));
        assertInstanceOf(PersistenceConsumer.PoisonRecord.class, failed.getCause());
        assertEquals(0, failed.getIndex());
      }
    } finally {
      meters.close();
    }
  }
}
