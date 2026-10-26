package dev.sharad.telemetry.schema;

import org.apache.avro.Schema;

/** Implementations must distinguish unavailable infrastructure from an invalid event. */
public interface SchemaCatalog {
  int registerOrLookup(String subject, Schema schema);

  Schema byId(int id);

  final class Unavailable extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public Unavailable(Throwable cause) {
      super("Schema registry unavailable", cause);
    }
  }

  final class Incompatible extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public Incompatible() {
      super("Registry rejected schema registration");
    }
  }
}
