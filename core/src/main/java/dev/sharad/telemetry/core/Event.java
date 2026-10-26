package dev.sharad.telemetry.core;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

/** Immutable, versioned event after transport authentication and validation. */
public record Event(
    UUID eventId,
    String tenantId,
    String eventType,
    int schemaVersion,
    Instant occurredAt,
    Map<String, String> attributes) {
  private static final Pattern TENANT = Pattern.compile("[A-Za-z0-9_-]{1,64}");
  private static final Instant EARLIEST = Instant.parse("0001-01-01T00:00:00Z");
  private static final Instant LATEST = Instant.parse("9999-12-31T23:59:59.999999999Z");
  private static final Set<String> TYPES =
      Set.of("session.started", "session.ended", "performance.sample");
  private static final Set<String> ATTRIBUTES =
      Set.of("platform", "region", "sdkVersion", "statusCode", "durationMs", "metric", "value");

  public Event {
    if (eventId == null || occurredAt == null) {
      throw new InvalidEvent("Event identifier and timestamp are required");
    }
    if (occurredAt.isBefore(EARLIEST) || occurredAt.isAfter(LATEST)) {
      throw new InvalidEvent("Timestamp must be within years 0001 through 9999");
    }
    if (tenantId == null || !TENANT.matcher(tenantId).matches()) {
      throw new InvalidEvent("Invalid tenant identifier");
    }
    if (eventType == null || !TYPES.contains(eventType)) {
      throw new InvalidEvent("Unsupported event type");
    }
    if (schemaVersion != 1) {
      throw new InvalidEvent("Unsupported schema version");
    }
    if (attributes == null || attributes.size() > 7) {
      throw new InvalidEvent("Invalid attributes");
    }
    for (var entry : attributes.entrySet()) {
      if (entry.getKey() == null
          || !ATTRIBUTES.contains(entry.getKey())
          || entry.getValue() == null
          || entry.getValue().length() > 128
          || !supportedText(entry.getValue())) {
        throw new InvalidEvent("Unsupported attribute or attribute length");
      }
    }
    attributes = Map.copyOf(attributes);
  }

  public String partitionKey() {
    return tenantId + ":" + eventId;
  }

  private static boolean supportedText(String value) {
    for (int index = 0; index < value.length(); index++) {
      char current = value.charAt(index);
      if (current == 0 || Character.isLowSurrogate(current)) return false;
      if (Character.isHighSurrogate(current)) {
        if (++index == value.length() || !Character.isLowSurrogate(value.charAt(index)))
          return false;
      }
    }
    return true;
  }

  /** Length-prefixed, sorted canonical encoding; independent of JSON map order. */
  public byte[] fingerprint() {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        write(output, eventId.toString());
        write(output, tenantId);
        write(output, eventType);
        output.writeInt(schemaVersion);
        write(output, occurredAt.toString());
        output.writeInt(attributes.size());
        for (var entry : new TreeMap<>(attributes).entrySet()) {
          write(output, entry.getKey());
          write(output, entry.getValue());
        }
      }
      return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
    } catch (IOException | NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("Canonical hashing unavailable", impossible);
    }
  }

  private static void write(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  public static final class InvalidEvent extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public InvalidEvent(String message) {
      super(message);
    }
  }
}
