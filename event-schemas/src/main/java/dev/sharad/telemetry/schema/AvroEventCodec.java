package dev.sharad.telemetry.schema;

import dev.sharad.telemetry.core.Event;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Registry wire format: magic byte 0, schema ID (big endian), Avro binary record. */
public final class AvroEventCodec {
  public static final String SUBJECT = "telemetry.events-value";
  private final SchemaCatalog catalog;
  private final Schema v1 = EventSchemas.v1();
  private final Schema v2 = EventSchemas.v2();
  private final int v1Id;
  private final int v2Id;

  public AvroEventCodec(SchemaCatalog catalog) {
    this.catalog = catalog;
    v1Id = catalog.registerOrLookup(SUBJECT, v1);
    v2Id = catalog.registerOrLookup(SUBJECT, v2);
  }

  public byte[] encode(Event event) {
    return encode(event, v2, v2Id);
  }

  public byte[] encodeLegacy(Event event) {
    return encode(event, v1, v1Id);
  }

  private byte[] encode(Event event, Schema schema, int id) {
    try {
      GenericRecord record = new GenericData.Record(schema);
      record.put("eventId", event.eventId().toString());
      record.put("tenantId", event.tenantId());
      record.put("eventType", event.eventType());
      record.put("schemaVersion", event.schemaVersion());
      record.put("occurredAt", event.occurredAt().toString());
      record.put("attributes", event.attributes());
      if (schema.getField("collectorSource") != null)
        record.put("collectorSource", "http-collector");
      var bytes = new ByteArrayOutputStream();
      bytes.write(0);
      bytes.write(ByteBuffer.allocate(4).putInt(id).array());
      var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
      new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
      encoder.flush();
      return bytes.toByteArray();
    } catch (IOException invalid) {
      throw new InvalidEnvelope();
    }
  }

  public Event decode(byte[] payload) {
    if (payload == null || payload.length < 6 || payload.length > 32768 || payload[0] != 0)
      throw new InvalidEnvelope();
    int id = ByteBuffer.wrap(payload, 1, 4).getInt();
    if (id < 1) throw new InvalidEnvelope();
    Schema writer = catalog.byId(id); // Infrastructure failures remain retryable, not poison.
    try {
      var decoder = DecoderFactory.get().binaryDecoder(payload, 5, payload.length - 5, null);
      GenericRecord record = new GenericDatumReader<GenericRecord>(writer, v2).read(null, decoder);
      if (!decoder.isEnd()) throw new InvalidEnvelope();
      var attributes = new HashMap<String, String>();
      ((Map<?, ?>) record.get("attributes"))
          .forEach((key, value) -> attributes.put(key.toString(), value.toString()));
      return new Event(
          UUID.fromString(record.get("eventId").toString()),
          record.get("tenantId").toString(),
          record.get("eventType").toString(),
          (Integer) record.get("schemaVersion"),
          Instant.parse(record.get("occurredAt").toString()),
          attributes);
    } catch (IOException | RuntimeException invalid) {
      throw new InvalidEnvelope();
    }
  }

  public static final class InvalidEnvelope extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InvalidEnvelope() {
      super("Unsupported event envelope");
    }
  }
}
