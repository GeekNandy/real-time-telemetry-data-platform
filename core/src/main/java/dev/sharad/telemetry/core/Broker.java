package dev.sharad.telemetry.core;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface Broker {
    record Position(String topic, int partition, long offset) {
        public Position {
            if (topic == null || topic.isBlank() || partition < 0 || offset < 0) {
                throw new IllegalArgumentException("Invalid broker position");
            }
        }
    }

    /** Completion means the configured broker acknowledgement policy was satisfied. */
    CompletionStage<Position> append(Event event) throws Exception;
}
