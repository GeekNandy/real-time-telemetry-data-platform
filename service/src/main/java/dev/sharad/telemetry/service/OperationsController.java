package dev.sharad.telemetry.service;

import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/** A bounded, fixed-name metrics view. It never queries storage or enumerates tenant data. */
@RestController
public class OperationsController {
  private final MeterRegistry meters;
  private final String component;
  private final Scheduler operationsScheduler;
  private final String instanceId = UUID.randomUUID().toString();

  public OperationsController(
      MeterRegistry meters,
      @Value("${telemetry.component:combined}") String component,
      @Qualifier("operationsScheduler") Scheduler operationsScheduler) {
    this.meters = meters;
    this.component = component;
    this.operationsScheduler = operationsScheduler;
  }

  @GetMapping("/v1/operations/metrics")
  public Mono<ResponseEntity<Snapshot>> metrics() {
    return Mono.fromCallable(this::readSnapshot)
        .subscribeOn(operationsScheduler)
        .timeout(Duration.ofSeconds(1))
        .map(value -> ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value))
        .onErrorResume(
            error ->
                error instanceof TimeoutException || error instanceof RejectedExecutionException,
            error ->
                Mono.just(
                    ResponseEntity.status(503)
                        .header("Retry-After", "1")
                        .cacheControl(CacheControl.noStore())
                        .build()));
  }

  private Snapshot readSnapshot() {
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
    return new Snapshot(
        component,
        instanceId,
        Instant.now(),
        accepted == null ? null : finite(accepted.count()),
        gauge("telemetry.buffer.records"),
        gauge("telemetry.buffer.bytes"),
        gauge("telemetry.buffer.oldest.age"),
        gauge("telemetry.spool.pending.writes"),
        gauge("telemetry.spool.pending.bytes"),
        Map.copyOf(stages));
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
      Double appendPendingRecords,
      Double appendPendingBytes,
      Map<String, StageSnapshot> stages) {}
}
