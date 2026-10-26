package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.schema.AvroEventCodec;
import dev.sharad.telemetry.schema.EventSchemas;
import dev.sharad.telemetry.schema.RegistryClient;
import dev.sharad.telemetry.schema.SchemaCatalog;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class RegistryContractIT {
  @Test
  void enforcesTransitiveCompatibilityAndRecoversTheSameIdsAfterProcessDeath() throws Exception {
    try (var infrastructure = new LocalBrokerRegistry()) {
      var registry = new RegistryClient(infrastructure.registryUrl(), true, "");
      registry.compatibility(AvroEventCodec.SUBJECT, "FULL_TRANSITIVE");
      int first = registry.registerOrLookup(AvroEventCodec.SUBJECT, EventSchemas.v1());
      int second = registry.registerOrLookup(AvroEventCodec.SUBJECT, EventSchemas.v2());
      assertNotEquals(first, second);
      var event =
          new Event(
              UUID.randomUUID(),
              "registry",
              "session.started",
              1,
              Instant.now(),
              Map.of("platform", "linux"));
      var codec = new AvroEventCodec(registry);
      byte[] payload = codec.encodeLegacy(event);
      assertEquals(event, codec.decode(payload));
      String invalid =
          EventSchemas.v2()
              .toString()
              .replace(
                  "\"fields\":[",
                  "\"fields\":[{\"name\":\"requiredNewField\",\"type\":\"string\"},");
      assertThrows(
          SchemaCatalog.Incompatible.class,
          () ->
              registry.registerOrLookup(
                  AvroEventCodec.SUBJECT, new Schema.Parser().parse(invalid)));
      infrastructure.stopRegistry();
      assertThrows(
          SchemaCatalog.Unavailable.class,
          () -> new RegistryClient(infrastructure.registryUrl(), false, "").byId(first));
      assertEquals(
          event,
          codec.decode(payload),
          "Warm validated schemas remain usable during registry outage");
      infrastructure.startRegistry();
      var restarted = new RegistryClient(infrastructure.registryUrl(), false, "");
      assertEquals(first, restarted.registerOrLookup(AvroEventCodec.SUBJECT, EventSchemas.v1()));
      assertEquals(second, restarted.registerOrLookup(AvroEventCodec.SUBJECT, EventSchemas.v2()));
      assertEquals(event, new AvroEventCodec(restarted).decode(payload));
    }
  }
}
