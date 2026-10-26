package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.DeliveryProcessor;
import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class PersistenceConsumer {
    private final DeliveryProcessor processor;
    private final JsonMapper mapper;

    public PersistenceConsumer(EventSink sink, JsonMapper mapper) {
        this.processor = new DeliveryProcessor(sink);
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${telemetry.topic}", groupId = "${telemetry.consumer-group}",
            concurrency = "3")
    public void consume(String payload, Acknowledgment acknowledgement) {
        Event event;
        try {
            event = mapper.readValue(payload, Event.class);
            if (event == null) throw new PoisonRecord();
        } catch (RuntimeException invalid) {
            // Do not put untrusted payloads into exception messages or logs.
            throw new PoisonRecord();
        }
        processor.process(event, acknowledgement::acknowledge);
    }

    public static final class PoisonRecord extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public PoisonRecord() { super("Unsupported event payload"); }
    }
}
