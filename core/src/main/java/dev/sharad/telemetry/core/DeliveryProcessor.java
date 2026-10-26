package dev.sharad.telemetry.core;

import java.util.Objects;

/** The sink transaction must finish before the consumer may commit its offset. */
public final class DeliveryProcessor {
    private final EventSink sink;
    public DeliveryProcessor(EventSink sink) { this.sink = Objects.requireNonNull(sink); }

    public EventSink.Outcome process(Event event, Runnable commitOffset) {
        EventSink.Outcome outcome = sink.persist(event);
        Objects.requireNonNull(commitOffset, "commitOffset").run();
        return outcome;
    }
}
