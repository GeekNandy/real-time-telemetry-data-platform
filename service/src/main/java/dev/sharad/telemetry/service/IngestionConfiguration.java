package dev.sharad.telemetry.service;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.config.PlatformSettings;
import dev.sharad.telemetry.core.DurableLog;
import dev.sharad.telemetry.core.IngestionEngine;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@Profile("!worker")
public class IngestionConfiguration {
  @Bean
  PlatformSettings platformSettings(
      @Value("${telemetry.global-capacity}") int capacity,
      @Value("${telemetry.tenant-capacity}") int tenantCapacity,
      @Value("${telemetry.send-threads}") int threads,
      @Value("${telemetry.queue-deadline-ms}") long queueDeadline,
      @Value("${telemetry.drain-deadline-ms}") long drainDeadline,
      @Value("${telemetry.buffer.max-bytes}") long bufferBytes,
      @Value("${telemetry.buffer.max-records}") int records,
      @Value("${telemetry.buffer.max-record-bytes}") int recordBytes) {
    return new PlatformSettings(
        capacity,
        tenantCapacity,
        threads,
        Duration.ofMillis(queueDeadline),
        Duration.ofMillis(drainDeadline),
        new PlatformSettings.Buffer(bufferBytes, records, recordBytes));
  }

  @Bean(destroyMethod = "close")
  DiskBuffer diskBuffer(@Value("${telemetry.buffer.path}") Path path, PlatformSettings settings)
      throws IOException {
    return new DiskBuffer(path, settings.buffer());
  }

  @Bean(destroyMethod = "")
  IngestionEngine ingestionEngine(
      DiskBuffer buffer, AvroEventCodec codec, PlatformSettings settings, MeterRegistry metrics) {
    var engine =
        new IngestionEngine(
            submission -> {
              var event = submission.event();
              var entry =
                  buffer.append(
                      event.partitionKey(), codec.encode(event), submission.traceHeaders());
              return CompletableFuture.completedFuture(
                  new DurableLog.Position("collector-volume", 0, entry.sequence()));
            },
            settings.globalCapacity(),
            settings.tenantCapacity(),
            settings.appendThreads(),
            settings.queueDeadline());
    metrics.gauge(
        "telemetry.ingestion.inflight", engine, value -> value.snapshot().admission().inFlight());
    metrics.gauge(
        "telemetry.ingestion.active.tenants",
        engine,
        value -> value.snapshot().admission().activeTenants());
    metrics.gauge("telemetry.buffer.records", buffer, value -> value.snapshot().records());
    metrics.gauge("telemetry.buffer.bytes", buffer, value -> value.snapshot().bytes());
    metrics.gauge(
        "telemetry.buffer.oldest.age",
        buffer,
        value -> value.snapshot().oldestAgeMillis() / 1000.0);
    FunctionCounter.builder(
            "telemetry.ingestion.accepted", engine, value -> value.snapshot().accepted())
        .register(metrics);
    FunctionCounter.builder(
            "telemetry.ingestion.failed", engine, value -> value.snapshot().failed())
        .register(metrics);
    FunctionCounter.builder(
            "telemetry.ingestion.rejected", engine, value -> value.snapshot().rejected())
        .register(metrics);
    return engine;
  }

  @Bean
  ApplicationListener<ContextClosedEvent> beginDrain(IngestionEngine engine) {
    return event -> engine.beginDrain();
  }

  @Bean
  DisposableBean finishDrain(IngestionEngine engine, PlatformSettings settings) {
    return () -> engine.close(settings.drainDeadline());
  }

  @Bean
  BufferDispatcher bufferDispatcher(
      DiskBuffer buffer,
      KafkaTemplate<String, byte[]> kafka,
      PlatformTelemetry telemetry,
      @Value("${telemetry.topic}") String topic,
      MeterRegistry meters) {
    return new BufferDispatcher(buffer, kafka, telemetry, topic, meters);
  }

  @Bean
  HealthIndicator collectorBufferHealthIndicator(
      DiskBuffer buffer, IngestionEngine engine, BufferDispatcher dispatcher) {
    return () -> {
      var snapshot = buffer.snapshot();
      boolean ready =
          engine.snapshot().admission().accepting()
              && dispatcher.fatalFailure() == null
              && snapshot.records() < snapshot.maxRecords()
              && snapshot.bytes() < snapshot.maxBytes();
      return ready ? Health.up().build() : Health.outOfService().build();
    };
  }
}
