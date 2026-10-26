package dev.sharad.telemetry.schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;

public final class EventSchemas {
  private EventSchemas() {}

  public static Schema v1() {
    return load("telemetry-v1.avsc");
  }

  public static Schema v2() {
    return load("telemetry-v2.avsc");
  }

  private static Schema load(String file) {
    try (var input = EventSchemas.class.getResourceAsStream("/avro/" + file)) {
      if (input == null) throw new IllegalStateException("Bundled event schema is missing");
      return new Schema.Parser().parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException failure) {
      throw new IllegalStateException("Cannot load bundled event schema", failure);
    }
  }
}
