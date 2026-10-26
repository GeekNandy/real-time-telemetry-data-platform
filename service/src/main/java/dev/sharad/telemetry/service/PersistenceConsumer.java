package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.DeliveryProcessor;
import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("!collector")
public class PersistenceConsumer {
  private final DeliveryProcessor processor;
  private final AvroEventCodec codec;
  private final PlatformTelemetry telemetry;

  public PersistenceConsumer(EventSink sink, AvroEventCodec codec, PlatformTelemetry telemetry) {
    this.processor = new DeliveryProcessor(sink);
    this.codec = codec;
    this.telemetry = telemetry;
  }

  @KafkaListener(
      topics = "${telemetry.topic}",
      groupId = "${telemetry.consumer-group}",
      concurrency = "3")
  public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgement) {
    Event event;
    try {
      event = codec.decode(record.value());
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
      processor.process(event, acknowledgement::acknowledge);
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
