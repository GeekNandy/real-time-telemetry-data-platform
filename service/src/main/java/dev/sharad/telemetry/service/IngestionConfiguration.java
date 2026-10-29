package dev.sharad.telemetry.service;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.config.PlatformSettings;
import dev.sharad.telemetry.core.IngestionEngine;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
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

  @Bean(destroyMethod = "close")
  KafkaAcceptance kafkaAcceptance(
      DiskBuffer buffer,
      PlatformSettings settings,
      @Value("${telemetry.acceptance-deadline-ms}") long deadline,
      MeterRegistry metrics) {
    var acceptance =
        new KafkaAcceptance(
            buffer, settings.globalCapacity(), Duration.ofMillis(deadline), metrics);
    metrics.gauge("telemetry.ingestion.waiting", acceptance, KafkaAcceptance::pendingWaiters);
    return acceptance;
  }

  @Bean(destroyMethod = "")
  IngestionEngine ingestionEngine(
      DiskBuffer buffer,
      KafkaAcceptance acceptance,
      AvroEventCodec codec,
      PlatformSettings settings,
      MeterRegistry metrics) {
    var encoding =
        Timer.builder("telemetry.append.phase.duration")
            .tag("phase", "encode")
            .publishPercentileHistogram()
            .register(metrics);
    var spooling =
        Timer.builder("telemetry.append.phase.duration")
            .tag("phase", "spool_enqueue")
            .publishPercentileHistogram()
            .register(metrics);
    var engine =
        new IngestionEngine(
            submission -> {
              var event = submission.event();
              byte[] encoded;
              long encodeStarted = System.nanoTime();
              try {
                encoded = codec.encode(event);
              } finally {
                encoding.record(
                    System.nanoTime() - encodeStarted, java.util.concurrent.TimeUnit.NANOSECONDS);
              }
              long appendStarted = System.nanoTime();
              try {
                return acceptance.append(event.partitionKey(), encoded, submission.traceHeaders());
              } finally {
                spooling.record(
                    System.nanoTime() - appendStarted, java.util.concurrent.TimeUnit.NANOSECONDS);
              }
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
      KafkaAcceptance acceptance,
      KafkaTemplate<String, byte[]> kafka,
      PlatformTelemetry telemetry,
      @Value("${telemetry.topic}") String topic,
      @Value("${telemetry.dispatch.batch-records:64}") int batchRecords,
      @Value("${telemetry.dispatch.batch-bytes:1048576}") long batchBytes,
      @Value("${telemetry.dispatch.publication-deadline-ms:10000}") long publicationDeadline,
      MeterRegistry meters) {
    return new BufferDispatcher(
        buffer,
        acceptance,
        kafka,
        telemetry,
        topic,
        meters,
        batchRecords,
        batchBytes,
        Duration.ofMillis(publicationDeadline));
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
