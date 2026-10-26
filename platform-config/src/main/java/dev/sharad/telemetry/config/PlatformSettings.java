package dev.sharad.telemetry.config;

import java.time.Duration;
import java.util.Objects;

/** Shared, transport-independent limits validated once at startup. */
public record PlatformSettings(
    int globalCapacity,
    int tenantCapacity,
    int appendThreads,
    Duration queueDeadline,
    Duration drainDeadline,
    Buffer buffer) {
  public PlatformSettings {
    if (globalCapacity < 1
        || tenantCapacity < 1
        || tenantCapacity > globalCapacity
        || appendThreads < 1) {
      throw new IllegalArgumentException(
          "Require 1 <= tenant capacity <= global capacity and positive workers");
    }
    requirePositive(queueDeadline, "queue deadline");
    requirePositive(drainDeadline, "drain deadline");
    Objects.requireNonNull(buffer, "buffer");
  }

  public record Buffer(long maxBytes, int maxRecords, int maxRecordBytes) {
    public Buffer {
      if (maxBytes < 1 || maxRecords < 1 || maxRecordBytes < 64 || maxRecordBytes > maxBytes) {
        throw new IllegalArgumentException("Invalid durable-buffer limits");
      }
      if (maxRecords > 100_000) throw new IllegalArgumentException("Too many buffer file entries");
    }
  }

  private static void requirePositive(Duration value, String name) {
    if (value == null || value.isZero() || value.isNegative())
      throw new IllegalArgumentException(name);
  }
}
