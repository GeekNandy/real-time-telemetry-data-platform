package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

class WireContractTest {
    private final JsonMapper mapper = new IngestionConfiguration().jsonMapper();

    @Test void roundTripsTheBrokerEnvelope() {
        var event = new Event(UUID.randomUUID(), "tenant-a", "performance.sample", 1,
                Instant.parse("2026-01-01T00:00:00.123456789Z"), Map.of("metric", "frame_ms", "value", "16"));
        assertEquals(event, mapper.readValue(mapper.writeValueAsString(event), Event.class));
    }

    @Test void refusesClientSuppliedTenant() {
        String json = """
                {"eventId":"00000000-0000-0000-0000-000000000001",
                 "eventType":"session.started","schemaVersion":1,
                 "occurredAt":"2026-01-01T00:00:00Z","attributes":{},"tenantId":"tenant-b"}
                """;
        assertThrows(RuntimeException.class, () -> mapper.readValue(json, EventRequest.class));
    }

    @Test void quarantinesInvalidBrokerPayloadBeforeTouchingStorage() {
        EventSink sink = event -> { throw new AssertionError("Invalid data reached persistence"); };
        Acknowledgment acknowledgment = () -> { throw new AssertionError("Invalid data committed its offset"); };
        var consumer = new PersistenceConsumer(sink, mapper);
        for (String payload : new String[] {"null", "{}", "{broken"}) {
            assertThrows(PersistenceConsumer.PoisonRecord.class, () -> consumer.consume(payload, acknowledgment));
        }
    }
}
