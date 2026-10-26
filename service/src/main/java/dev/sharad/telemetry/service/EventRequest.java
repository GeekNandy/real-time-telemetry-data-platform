package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.Event;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** tenantId is deliberately absent: ownership comes exclusively from the verified JWT. */
public record EventRequest(@NotNull UUID eventId, @NotNull String eventType, int schemaVersion,
                           @NotNull Instant occurredAt, @NotNull Map<String, String> attributes) {
    public Event forTenant(String verifiedTenant) {
        return new Event(eventId, verifiedTenant, eventType, schemaVersion, occurredAt, attributes);
    }
}
