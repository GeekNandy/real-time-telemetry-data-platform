package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Repository
@DependsOn("flyway")
public class PostgresEventSink implements EventSink {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final JsonMapper mapper;

    public PostgresEventSink(JdbcTemplate jdbc, TransactionTemplate transaction, JsonMapper mapper) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.mapper = mapper;
    }

    @Override
    public Outcome persist(Event event) {
        return transaction.execute(status -> {
            byte[] incoming = event.fingerprint();
            int inserted = jdbc.update("""
                    INSERT INTO telemetry_events
                    (tenant_id, event_id, event_type, schema_version, occurred_at, attributes, fingerprint)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb, ?)
                    ON CONFLICT (tenant_id, event_id) DO NOTHING
                    """, event.tenantId(), event.eventId(), event.eventType(), event.schemaVersion(),
                    Timestamp.from(event.occurredAt()), mapper.writeValueAsString(event.attributes()), incoming);
            if (inserted == 1) return Outcome.INSERTED;
            byte[] existing = jdbc.queryForObject("""
                    SELECT fingerprint FROM telemetry_events WHERE tenant_id = ? AND event_id = ?
                    """, byte[].class, event.tenantId(), event.eventId());
            if (existing == null || !MessageDigest.isEqual(existing, incoming)) throw new EventSink.Collision();
            return Outcome.DUPLICATE;
        });
    }

    public Optional<Map<String, Object>> find(String tenant, UUID eventId) {
        return jdbc.query("""
                SELECT event_id, event_type, schema_version, occurred_at, attributes::text
                FROM telemetry_events WHERE tenant_id = ? AND event_id = ?
                """, (row, index) -> Map.<String, Object>of(
                "eventId", row.getObject("event_id", UUID.class),
                "eventType", row.getString("event_type"),
                "schemaVersion", row.getInt("schema_version"),
                "occurredAt", row.getTimestamp("occurred_at").toInstant().toString(),
                "attributes", mapper.readValue(row.getString("attributes"), Map.class)),
                tenant, eventId).stream().findFirst();
    }
}
