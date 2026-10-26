package dev.sharad.telemetry.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("!collector")
public class RetentionMaintenance {
  private final JdbcTemplate jdbc;
  private final Duration retention;
  private final Counter removed;

  public RetentionMaintenance(
      JdbcTemplate jdbc,
      MeterRegistry meters,
      @Value("${telemetry.retention.database-days:30}") int days) {
    if (days < 1) throw new IllegalArgumentException("Retention must be positive");
    this.jdbc = jdbc;
    this.retention = Duration.ofDays(days);
    removed = Counter.builder("telemetry.retention.removed").register(meters);
  }

  @Scheduled(fixedDelayString = "${telemetry.retention.sweep-ms:60000}")
  public int purge() {
    int count =
        jdbc.update(
            """
            WITH expired AS (
                SELECT tenant_id,event_id FROM telemetry_events
                WHERE stored_at < ? ORDER BY stored_at LIMIT 1000 FOR UPDATE SKIP LOCKED
            )
            DELETE FROM telemetry_events target USING expired
            WHERE target.tenant_id=expired.tenant_id AND target.event_id=expired.event_id
            """,
            Timestamp.from(Instant.now().minus(retention)));
    removed.increment(count);
    return count;
  }
}
