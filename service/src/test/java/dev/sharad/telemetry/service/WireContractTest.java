package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.core.Event;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class WireContractTest {
  private final JsonMapper mapper = new RuntimeConfiguration().jsonMapper();

  @Test
  void roundTripsTheEventJsonValues() {
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
}
