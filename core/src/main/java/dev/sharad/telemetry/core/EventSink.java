package dev.sharad.telemetry.core;

@FunctionalInterface
public interface EventSink {
    enum Outcome { INSERTED, DUPLICATE }

    /** Must return only after the transaction commits, with an atomic idempotency check. */
    Outcome persist(Event event);

    final class Collision extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public Collision() { super("Event identity already belongs to a different payload"); }
    }
}
