package dev.sharad.telemetry.service;

import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** A bounded, fixed-name metrics view. It never queries storage or enumerates tenant data. */
@RestController
public class OperationsController {
  private final MeterRegistry meters;
  private final String component;
  private final String instanceId = UUID.randomUUID().toString();

  public OperationsController(
      MeterRegistry meters, @Value("${telemetry.component:combined}") String component) {
    this.meters = meters;
    this.component = component;
  }

  @GetMapping("/v1/operations/metrics")
  public ResponseEntity<Snapshot> metrics() {
    var stages = new LinkedHashMap<String, StageSnapshot>();
    for (var stage : PlatformTelemetry.Stage.values()) {
      String name = stage.name().toLowerCase(java.util.Locale.ROOT);
      var timer = meters.find("telemetry.stage.duration").tag("stage", name).timer();
      var failures = meters.find("telemetry.stage.failures").tag("stage", name).counter();
      stages.put(
          name,
          new StageSnapshot(
              timer == null ? null : timer.count(),
              failures == null ? null : finite(failures.count())));
    }
    var accepted = meters.find("telemetry.ingestion.accepted").functionCounter();
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore())
        .body(
            new Snapshot(
                component,
                instanceId,
                Instant.now(),
                accepted == null ? null : finite(accepted.count()),
                gauge("telemetry.buffer.records"),
                gauge("telemetry.buffer.bytes"),
                gauge("telemetry.buffer.oldest.age"),
                Map.copyOf(stages)));
  }

  private Double gauge(String name) {
    var gauge = meters.find(name).gauge();
    return gauge == null ? null : finite(gauge.value());
  }

  private static Double finite(double value) {
    return Double.isFinite(value) && value >= 0 ? value : null;
  }

  public record StageSnapshot(Long completed, Double failures) {}

  public record Snapshot(
      String component,
      String instanceId,
      Instant observedAt,
      Double acceptedTotal,
      Double bufferRecords,
      Double bufferBytes,
      Double oldestBufferedAgeSeconds,
      Map<String, StageSnapshot> stages) {}
}
