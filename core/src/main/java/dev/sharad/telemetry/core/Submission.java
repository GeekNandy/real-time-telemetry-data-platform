package dev.sharad.telemetry.core;

import java.util.Map;
import java.util.Objects;

/** Immutable event and bounded trace context carried across asynchronous work. */
public record Submission(Event event, Map<String, String> traceHeaders) {
  public Submission {
    Objects.requireNonNull(event, "event");
    if (traceHeaders == null || traceHeaders.size() > 2)
      throw new IllegalArgumentException("Invalid trace metadata");
    traceHeaders.forEach(
        (key, value) -> {
          if (key == null
              || (!key.equals("traceparent") && !key.equals("tracestate"))
              || value == null
              || value.length() > 512
              || value.chars().anyMatch(c -> c < 32 || c > 126))
            throw new IllegalArgumentException("Unsupported trace metadata");
        });
    traceHeaders = Map.copyOf(traceHeaders);
  }
}
