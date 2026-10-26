package dev.sharad.telemetry.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** The same independent contract cases run through the JDK runner and JUnit. */
public final class CoreContractCases {
    @FunctionalInterface public interface CheckedRunnable { void run() throws Exception; }
    public record Case(String name, CheckedRunnable body) { }

    public static List<Case> cases() {
        return List.of(
                new Case("unsupported_schema_is_rejected", CoreContractCases::invalidSchema),
                new Case("missing_required_event_fields_are_rejected", CoreContractCases::missingFields),
                new Case("unsupported_storage_timestamps_are_rejected", CoreContractCases::timestampRange),
                new Case("unsupported_storage_text_is_rejected", CoreContractCases::storageText),
                new Case("unapproved_attribute_is_rejected", CoreContractCases::unapprovedAttribute),
                new Case("event_is_immutable", CoreContractCases::immutableEvent),
                new Case("fingerprint_ignores_map_iteration_order", CoreContractCases::canonicalFingerprint),
                new Case("invalid_admission_configuration_is_rejected", CoreContractCases::invalidCapacity),
                new Case("global_and_tenant_limits_are_atomic", CoreContractCases::admissionLimits),
                new Case("lease_release_is_idempotent", CoreContractCases::leaseRelease),
                new Case("acceptance_waits_for_broker", CoreContractCases::ackBoundary),
                new Case("client_cancellation_keeps_real_send_admitted", CoreContractCases::cancellation),
                new Case("broker_failure_releases_capacity", CoreContractCases::asyncFailure),
                new Case("synchronous_send_failure_releases_capacity", CoreContractCases::syncFailure),
                new Case("broken_broker_contract_releases_capacity", CoreContractCases::nullBrokerStage),
                new Case("busy_tenant_does_not_consume_other_tenant_capacity", CoreContractCases::tenantIsolation),
                new Case("drain_rejects_new_work_and_waits_for_actual_ack", CoreContractCases::draining),
                new Case("shutdown_deadline_does_not_claim_acceptance", CoreContractCases::shutdownDeadline),
                new Case("concurrent_load_respects_global_capacity", CoreContractCases::concurrentBound),
                new Case("offset_is_committed_after_persistence", CoreContractCases::commitOrdering),
                new Case("replay_after_commit_gap_is_idempotent", CoreContractCases::crashWindow),
                new Case("storage_failure_does_not_commit_offset", CoreContractCases::storageFailure),
                new Case("payload_collision_does_not_commit_offset", CoreContractCases::collision),
                new Case("tenant_is_part_of_the_idempotency_key", CoreContractCases::tenantKey),
                new Case("randomized_replay_preserves_unique_event_set", CoreContractCases::randomReplay));
    }

    public static void main(String[] args) throws Exception {
        long started = System.nanoTime();
        int passed = 0;
        for (Case scenario : cases()) {
            try {
                scenario.body().run();
                passed++;
                System.out.println("PASS " + scenario.name());
            } catch (Exception | AssertionError error) {
                System.err.println("FAIL " + scenario.name());
                throw error;
            }
        }
        System.out.printf("{\"suite\":\"core-contracts\",\"passed\":%d,\"failed\":0,\"elapsedMs\":%.3f}%n",
                passed, (System.nanoTime() - started) / 1_000_000.0);
    }

    private static Event event(String tenant) {
        return new Event(UUID.randomUUID(), tenant, "session.started", 1,
                Instant.parse("2026-10-25T10:00:00Z"), Map.of("platform", "linux"));
    }

    private static void invalidSchema() {
        expect(Event.InvalidEvent.class, () -> new Event(UUID.randomUUID(), "a", "session.started",
                2, Instant.now(), Map.of()));
    }

    private static void unapprovedAttribute() {
        expect(Event.InvalidEvent.class, () -> new Event(UUID.randomUUID(), "a", "session.started",
                1, Instant.now(), Map.of("email", "synthetic@example.test")));
    }

    private static void missingFields() {
        UUID id = UUID.randomUUID();
        Instant time = Instant.now();
        expect(Event.InvalidEvent.class, () -> new Event(null, "a", "session.started", 1, time, Map.of()));
        expect(Event.InvalidEvent.class, () -> new Event(id, "a", null, 1, time, Map.of()));
        expect(Event.InvalidEvent.class, () -> new Event(id, "a", "session.started", 1, null, Map.of()));
        var attributes = new HashMap<String, String>();
        attributes.put(null, "invalid");
        expect(Event.InvalidEvent.class, () -> new Event(id, "a", "session.started", 1, time, attributes));
    }

    private static void timestampRange() {
        for (Instant timestamp : new Instant[] {Instant.MIN, Instant.MAX}) {
            expect(Event.InvalidEvent.class, () -> new Event(UUID.randomUUID(), "a", "session.started",
                    1, timestamp, Map.of()));
        }
    }

    private static void storageText() {
        for (String text : new String[] {String.valueOf((char) 0), String.valueOf((char) 0xD800),
                String.valueOf((char) 0xDC00)}) {
            expect(Event.InvalidEvent.class, () -> new Event(UUID.randomUUID(), "a", "session.started",
                    1, Instant.now(), Map.of("platform", text)));
        }
        new Event(UUID.randomUUID(), "a", "session.started", 1, Instant.now(),
                Map.of("platform", new String(Character.toChars(0x1F680))));
    }

    private static void immutableEvent() {
        var values = new HashMap<>(Map.of("platform", "linux"));
        var event = new Event(UUID.randomUUID(), "a", "session.started", 1, Instant.now(), values);
        values.put("platform", "changed");
        check(event.attributes().get("platform").equals("linux"), "Caller mutated the event");
        expect(UnsupportedOperationException.class, () -> event.attributes().put("region", "eu"));
    }

    private static void canonicalFingerprint() {
        UUID id = UUID.randomUUID();
        Instant time = Instant.now();
        var a = new Event(id, "a", "session.started", 1, time, Map.of("region", "eu", "platform", "linux"));
        var reverse = new java.util.LinkedHashMap<String, String>();
        reverse.put("platform", "linux"); reverse.put("region", "eu");
        var b = new Event(id, "a", "session.started", 1, time, reverse);
        check(Arrays.equals(a.fingerprint(), b.fingerprint()), "Map ordering changed the fingerprint");
    }

    private static void invalidCapacity() {
        expect(IllegalArgumentException.class, () -> new AdmissionController(0, 1));
        expect(IllegalArgumentException.class, () -> new AdmissionController(2, 3));
    }

    private static void admissionLimits() {
        var gate = new AdmissionController(3, 2);
        var a = gate.acquire("a"); var b = gate.acquire("a"); var c = gate.acquire("b");
        check(expect(AdmissionController.Rejected.class, () -> gate.acquire("c")).reason()
                == AdmissionController.Reason.GLOBAL_CAPACITY, "Wrong global rejection");
        c.close();
        check(expect(AdmissionController.Rejected.class, () -> gate.acquire("a")).reason()
                == AdmissionController.Reason.TENANT_CAPACITY, "Wrong tenant rejection");
        a.close(); b.close();
        check(gate.snapshot().inFlight() == 0 && gate.snapshot().activeTenants() == 0, "Leaked tenant entries");
    }

    private static void leaseRelease() {
        var gate = new AdmissionController(1, 1);
        var lease = gate.acquire("a"); lease.close(); lease.close();
        check(gate.snapshot().inFlight() == 0, "Double release changed accounting");
    }

    private static void ackBoundary() throws Exception {
        var broker = new ControlledBroker();
        try (var engine = new IngestionEngine(broker, 2, 1, 1)) {
            Event event = event("a");
            var result = engine.accept(event).toCompletableFuture();
            var pending = broker.next();
            check(!result.isDone(), "Accepted before broker acknowledgement");
            pending.complete(new Broker.Position("events", 1, 42));
            var receipt = result.get(2, TimeUnit.SECONDS);
            check(receipt.eventId().equals(event.eventId()) && receipt.position().offset() == 42, "Incorrect receipt");
            check(engine.snapshot().admission().inFlight() == 0, "Capacity not returned before acceptance");
        }
    }

    private static void cancellation() throws Exception {
        var broker = new ControlledBroker();
        try (var engine = new IngestionEngine(broker, 1, 1, 1)) {
            var view = engine.accept(event("a")).toCompletableFuture();
            var pending = broker.next();
            view.cancel(true);
            expect(AdmissionController.Rejected.class, () -> engine.accept(event("b")));
            check(!pending.isCancelled(), "Caller cancelled the actual broker operation");
            pending.complete(new Broker.Position("events", 0, 1));
            check(engine.snapshot().admission().inFlight() == 0, "Cancelled client leaked capacity");
        }
    }

    private static void asyncFailure() throws Exception {
        var broker = new ControlledBroker();
        try (var engine = new IngestionEngine(broker, 1, 1, 1)) {
            var result = engine.accept(event("a")).toCompletableFuture();
            broker.next().completeExceptionally(new IllegalStateException("transport failed"));
            expect(java.util.concurrent.ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
            check(engine.snapshot().accepted() == 0 && engine.snapshot().failed() == 1, "Failure counted as acceptance");
            check(engine.snapshot().admission().inFlight() == 0, "Failure leaked capacity");
        }
    }

    private static void syncFailure() throws Exception {
        try (var engine = new IngestionEngine(event -> { throw new IllegalStateException("send failed"); }, 1, 1, 1)) {
            var result = engine.accept(event("a")).toCompletableFuture();
            expect(java.util.concurrent.ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
            check(engine.snapshot().admission().inFlight() == 0, "Synchronous failure leaked capacity");
        }
    }

    private static void nullBrokerStage() throws Exception {
        try (var engine = new IngestionEngine(event -> null, 1, 1, 1)) {
            var result = engine.accept(event("a")).toCompletableFuture();
            expect(java.util.concurrent.ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
            check(engine.snapshot().admission().inFlight() == 0, "Invalid stage leaked capacity");
        }
    }

    private static void tenantIsolation() throws Exception {
        var broker = new ControlledBroker();
        try (var engine = new IngestionEngine(broker, 2, 1, 1)) {
            var a = engine.accept(event("a")).toCompletableFuture(); var first = broker.next();
            expect(AdmissionController.Rejected.class, () -> engine.accept(event("a")));
            var b = engine.accept(event("b")).toCompletableFuture(); var second = broker.next();
            first.complete(new Broker.Position("events", 0, 1)); second.complete(new Broker.Position("events", 1, 1));
            a.get(2, TimeUnit.SECONDS); b.get(2, TimeUnit.SECONDS);
        }
    }

    private static void draining() throws Exception {
        var broker = new ControlledBroker();
        try (var engine = new IngestionEngine(broker, 1, 1, 1)) {
            var result = engine.accept(event("a")).toCompletableFuture(); var pending = broker.next();
            var drain = engine.beginDrain().toCompletableFuture();
            check(!drain.isDone(), "Drain completed before the send");
            check(expect(AdmissionController.Rejected.class, () -> engine.accept(event("b"))).reason()
                    == AdmissionController.Reason.DRAINING, "Drain admitted new work");
            pending.complete(new Broker.Position("events", 0, 1));
            drain.get(2, TimeUnit.SECONDS); result.get(2, TimeUnit.SECONDS);
        }
    }

    private static void shutdownDeadline() throws Exception {
        var broker = new ControlledBroker();
        var engine = new IngestionEngine(broker, 1, 1, 1);
        var result = engine.accept(event("a")).toCompletableFuture(); var pending = broker.next();
        check(!engine.close(Duration.ofMillis(10)), "Unconfirmed send reported as drained");
        check(!result.isDone(), "Deadline manufactured an acknowledgement");
        pending.complete(new Broker.Position("events", 0, 1));
        result.get(2, TimeUnit.SECONDS); engine.close();
    }

    private static void concurrentBound() throws Exception {
        int capacity = 8;
        var broker = new ControlledBroker();
        var submissions = Executors.newFixedThreadPool(16);
        try (var engine = new IngestionEngine(broker, capacity, capacity, 2)) {
            var admitted = Collections.synchronizedList(new ArrayList<CompletableFuture<IngestionEngine.Acceptance>>());
            var done = new CountDownLatch(200);
            for (int i = 0; i < 200; i++) submissions.execute(() -> {
                try { admitted.add(engine.accept(event("a")).toCompletableFuture()); }
                catch (AdmissionController.Rejected expected) { }
                finally { done.countDown(); }
            });
            check(done.await(3, TimeUnit.SECONDS), "Concurrent submissions stalled");
            check(admitted.size() == capacity, "Admission bound violated");
            check(engine.snapshot().admission().inFlight() == capacity, "Accounting disagrees with admitted work");
            for (int i = 0; i < capacity; i++) broker.next().complete(new Broker.Position("events", 0, i));
            for (var future : admitted) future.get(2, TimeUnit.SECONDS);
            check(engine.snapshot().accepted() == capacity && engine.snapshot().rejected() == 192, "Outcome counts lost requests");
        } finally { submissions.shutdownNow(); }
    }

    private static void commitOrdering() {
        var order = new ArrayList<String>();
        var processor = new DeliveryProcessor(event -> { order.add("persist"); return EventSink.Outcome.INSERTED; });
        processor.process(event("a"), () -> order.add("commit"));
        check(order.equals(List.of("persist", "commit")), "Offset committed before persistence");
    }

    private static void crashWindow() {
        var sink = new MemorySink();
        var processor = new DeliveryProcessor(sink);
        Event event = event("a");
        expect(IllegalStateException.class, () -> processor.process(event, () -> { throw new IllegalStateException("crash"); }));
        var outcome = processor.process(event, () -> { });
        check(outcome == EventSink.Outcome.DUPLICATE && sink.size() == 1, "Crash replay duplicated stored data");
    }

    private static void storageFailure() {
        var commits = new AtomicInteger();
        var processor = new DeliveryProcessor(event -> { throw new IllegalStateException("storage unavailable"); });
        expect(IllegalStateException.class, () -> processor.process(event("a"), commits::incrementAndGet));
        check(commits.get() == 0, "Failed persistence advanced the offset");
    }

    private static void collision() {
        var sink = new MemorySink();
        var processor = new DeliveryProcessor(sink);
        Event a = event("a");
        processor.process(a, () -> { });
        var changed = new Event(a.eventId(), a.tenantId(), a.eventType(), 1, a.occurredAt(), Map.of("platform", "other"));
        var commits = new AtomicInteger();
        expect(EventSink.Collision.class, () -> processor.process(changed, commits::incrementAndGet));
        check(commits.get() == 0 && sink.size() == 1, "Conflicting payload changed the accepted record");
    }

    private static void tenantKey() {
        var sink = new MemorySink();
        Event a = event("a");
        Event b = new Event(a.eventId(), "b", a.eventType(), 1, a.occurredAt(), a.attributes());
        sink.persist(a); sink.persist(b);
        check(sink.size() == 2, "Cross-tenant events were deduplicated together");
    }

    private static void randomReplay() {
        var sink = new MemorySink();
        var processor = new DeliveryProcessor(sink);
        var events = new ArrayList<Event>();
        for (int i = 0; i < 500; i++) events.add(event("tenant_" + i % 7));
        var delivery = new ArrayList<>(events);
        delivery.addAll(events); delivery.addAll(events.subList(0, 200));
        Collections.shuffle(delivery, new Random(42));
        for (Event event : delivery) processor.process(event, () -> { });
        check(sink.size() == events.size(), "Replay changed the unique event set");
    }

    private static final class ControlledBroker implements Broker {
        private final BlockingQueue<CompletableFuture<Position>> pending = new LinkedBlockingQueue<>();
        @Override public CompletableFuture<Position> append(Event event) {
            var result = new CompletableFuture<Position>(); pending.add(result); return result;
        }
        private CompletableFuture<Position> next() throws InterruptedException {
            var result = pending.poll(3, TimeUnit.SECONDS);
            if (result == null) throw new AssertionError("Send was never initiated");
            return result;
        }
    }

    private static final class MemorySink implements EventSink {
        private final ConcurrentHashMap<String, byte[]> rows = new ConcurrentHashMap<>();
        @Override public Outcome persist(Event event) {
            byte[] incoming = event.fingerprint();
            byte[] existing = rows.putIfAbsent(event.partitionKey(), incoming);
            if (existing == null) return Outcome.INSERTED;
            if (!Arrays.equals(existing, incoming)) throw new EventSink.Collision();
            return Outcome.DUPLICATE;
        }
        int size() { return rows.size(); }
    }

    private static void check(boolean condition, String detail) {
        if (!condition) throw new AssertionError(detail);
    }

    private static <T extends Throwable> T expect(Class<T> type, CheckedRunnable action) {
        try { action.run(); }
        catch (Throwable failure) {
            if (type.isInstance(failure)) return type.cast(failure);
            throw new AssertionError("Expected " + type.getName() + ", got " + failure, failure);
        }
        throw new AssertionError("Expected " + type.getName());
    }
}
