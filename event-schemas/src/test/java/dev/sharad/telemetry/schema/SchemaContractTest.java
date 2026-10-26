package dev.sharad.telemetry.schema;

import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.core.Event;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.Test;

class SchemaContractTest {
  @Test
  void newerReadersCanReadBothPublishedVersions() {
    assertEquals(
        SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
        SchemaCompatibility.checkReaderWriterCompatibility(EventSchemas.v2(), EventSchemas.v1())
            .getType());
    assertEquals(
        SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
        SchemaCompatibility.checkReaderWriterCompatibility(EventSchemas.v1(), EventSchemas.v2())
            .getType());
  }

  @Test
  void rejectsARequiredFieldWithoutADefault() {
    String breaking =
        EventSchemas.v2()
            .toString()
            .replace("\"fields\":[", "\"fields\":[{\"name\":\"newRequired\",\"type\":\"string\"},");
    assertEquals(
        SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE,
        SchemaCompatibility.checkReaderWriterCompatibility(
                new Schema.Parser().parse(breaking), EventSchemas.v1())
            .getType());
  }

  @Test
  void decodesLegacyAndCurrentWireDataWithTheCurrentReader() {
    var codec = new AvroEventCodec(catalog());
    var event =
        new Event(
            UUID.randomUUID(),
            "a",
            "session.started",
            1,
            Instant.parse("2026-01-01T00:00:00.123456789Z"),
            Map.of("platform", "linux"));
    assertEquals(event, codec.decode(codec.encodeLegacy(event)));
    assertEquals(event, codec.decode(codec.encode(event)));
  }

  @Test
  void unavailableRegistryMustNotBeClassifiedAsPoison() {
    var codec =
        new AvroEventCodec(
            new SchemaCatalog() {
              public int registerOrLookup(String subject, Schema schema) {
                return 1;
              }

              public Schema byId(int id) {
                throw new SchemaCatalog.Unavailable(new java.io.IOException("offline"));
              }
            });
    assertThrows(
        SchemaCatalog.Unavailable.class, () -> codec.decode(new byte[] {0, 0, 0, 0, 1, 0}));
  }

  @Test
  void rejectsTruncatedOrForeignWireData() {
    var codec = new AvroEventCodec(catalog());
    assertThrows(AvroEventCodec.InvalidEnvelope.class, () -> codec.decode(new byte[] {1, 2, 3}));
    assertThrows(
        AvroEventCodec.InvalidEnvelope.class, () -> codec.decode(new byte[] {0, 0, 0, 0, 2, 1}));
  }

  private static SchemaCatalog catalog() {
    return new SchemaCatalog() {
      private final Map<Integer, Schema> schemas = new java.util.HashMap<>();

      public int registerOrLookup(String subject, Schema schema) {
        int id = schemas.size() + 1;
        schemas.put(id, schema);
        return id;
      }

      public Schema byId(int id) {
        return schemas.get(id);
      }
    };
  }
}
