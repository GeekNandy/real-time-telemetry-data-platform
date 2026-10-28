package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("!collector")
public class PersistenceConsumer {
  private final EventSink sink;
  private final Function<byte[], Event> decoder;
  private final Timer commits;
  private final DistributionSummary batches;
  private final PlatformTelemetry telemetry;

  @Autowired
  public PersistenceConsumer(
      EventSink sink, AvroEventCodec codec, PlatformTelemetry telemetry, MeterRegistry meters) {
    this(sink, codec::decode, telemetry, meters);
  }

  PersistenceConsumer(
      EventSink sink,
      Function<byte[], Event> decoder,
      PlatformTelemetry telemetry,
      MeterRegistry meters) {
    this.sink = sink;
    this.decoder = decoder;
    this.telemetry = telemetry;
    commits =
        Timer.builder("telemetry.persistence.batch.commit")
            .minimumExpectedValue(Duration.ofNanos(100_000))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentileHistogram()
            .register(meters);
    batches =
        DistributionSummary.builder("telemetry.persistence.batch.size")
            .minimumExpectedValue(1.0)
            .maximumExpectedValue(256.0)
            .publishPercentileHistogram()
            .register(meters);
  }

  @KafkaListener(
      topics = "${telemetry.topic}",
      groupId = "${telemetry.consumer-group}",
      concurrency = "3")
  public void consume(
      List<ConsumerRecord<String, byte[]>> records, Acknowledgment acknowledgement) {
    if (records.isEmpty()) return;
    if (records.size() > 256)
      throw new BatchListenerFailedException("Persistence batch exceeds limit", 0);
    batches.record(records.size());
    // Sequential transactions make every preceding index safe to checkpoint on partial failure.
    for (int index = 0; index < records.size(); index++) {
      try {
        persist(records.get(index));
      } catch (RuntimeException failure) {
        throw new BatchListenerFailedException(
            "Persistence batch record unconfirmed", failure, index);
      }
    }
    // One synchronous offset commit after the complete poll batch has reached durable storage.
    commits.record((Runnable) acknowledgement::acknowledge);
  }

  private void persist(ConsumerRecord<String, byte[]> record) {
    Event event;
    try {
      event = decoder.apply(record.value());
      if (!event.partitionKey().equals(record.key())) throw new PoisonRecord();
    } catch (AvroEventCodec.InvalidEnvelope invalid) {
      throw new PoisonRecord();
    }
    // Registry outage exceptions are deliberately not converted to poison records.
    var parent = new HashMap<String, String>();
    for (String key : new String[] {"traceparent", "tracestate"}) {
      var header = record.headers().lastHeader(key);
      if (header != null && header.value() != null && header.value().length <= 512)
        parent.put(key, new String(header.value(), StandardCharsets.UTF_8));
    }
    var span = telemetry.start(PlatformTelemetry.Stage.PERSISTENCE, SpanKind.CONSUMER, parent);
    long started = System.nanoTime();
    boolean success = false;
    try {
      sink.persist(event);
      success = true;
    } catch (RuntimeException failure) {
      span.setStatus(StatusCode.ERROR, "Persistence unconfirmed");
      throw failure;
    } finally {
      telemetry.completed(PlatformTelemetry.Stage.PERSISTENCE, started, success);
      span.end();
    }
  }

  public static final class PoisonRecord extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PoisonRecord() {
      super("Unsupported event payload or partition key");
    }
  }
}
