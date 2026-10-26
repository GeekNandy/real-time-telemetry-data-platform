package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/** Real HTTP, signature verification, Kafka and PostgreSQL. Docker is required, never silently skipped. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PlatformIT {
    private static final String TOPIC = "telemetry.events.v1";
    private static final String DLT = TOPIC + ".dlt";
    private static final String GROUP = "telemetry-persistence-v1";

    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("telemetry").withUsername("telemetry").withPassword("integration-test-only");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("spring.datasource.url", () -> POSTGRES.getJdbcUrl()
                + "?connectTimeout=2&socketTimeout=5&options=-c%20statement_timeout=3000");
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
        properties.add("telemetry.security.mode", () -> "demo");
        properties.add("telemetry.security.issuer", () -> TestTokens.ISSUER);
        properties.add("telemetry.security.demo-secret", () -> TestTokens.SECRET);
    }

    @Value("${local.server.port}") int port;
    @Autowired JsonMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired PostgresEventSink sink;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaListenerEndpointRegistry listeners;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Test @Order(1) void acceptsThenPersistsAndDeduplicates() throws Exception {
        var event = event("tenant-a");
        assertEquals(202, post(event, TestTokens.tenant("tenant-a")).statusCode());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertEquals(1, rows(event)));
        for (int attempt = 0; attempt < 3; attempt++) {
            assertEquals(202, post(event, TestTokens.tenant("tenant-a")).statusCode());
        }
        var receipt = post(event, TestTokens.tenant("tenant-a"));
        assertEquals(202, receipt.statusCode());
        awaitCommitted(receipt);
        assertEquals(1, rows(event));
        assertEquals(200, get(event.eventId(), "tenant-a").statusCode());
        assertEquals(404, get(event.eventId(), "tenant-b").statusCode());
    }

    @Test @Order(2) void requiresAuthenticationScopeAndValidContract() throws Exception {
        var event = event("tenant-a");
        assertEquals(401, request("/v1/events", body(event), null).statusCode());
        String readOnly = TestTokens.sign(TestTokens.claims("tenant-a").claim("scope", "telemetry.read").build(),
                TestTokens.SECRET);
        assertEquals(403, request("/v1/events", body(event), readOnly).statusCode());
        assertEquals(400, request("/v1/events", body(event).replace("\"schemaVersion\":1",
                "\"schemaVersion\":99"), TestTokens.tenant("tenant-a")).statusCode());
        String spoofed = body(event).replaceFirst("\\{", "{\"tenantId\":\"tenant-b\",");
        assertEquals(400, request("/v1/events", spoofed, TestTokens.tenant("tenant-a")).statusCode());
        String oversized = body(event).replace("linux", "x".repeat(20_000));
        assertEquals(413, request("/v1/events", oversized, TestTokens.tenant("tenant-a")).statusCode());
        assertEquals(0, rows(event));
    }

    @Test @Order(3) void reconcilesTheExactAcceptedIdSet() throws Exception {
        var accepted = new ArrayList<UUID>();
        for (int index = 0; index < 40; index++) {
            var event = event("reconciliation");
            assertEquals(202, post(event, TestTokens.tenant("reconciliation")).statusCode());
            accepted.add(event.eventId());
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<UUID> stored = jdbc.query("SELECT event_id FROM telemetry_events WHERE tenant_id = ?",
                    (row, index) -> row.getObject(1, UUID.class), "reconciliation");
            assertEquals(accepted.size(), stored.size());
            assertEquals(new java.util.HashSet<>(accepted), new java.util.HashSet<>(stored));
        });
    }

    @Test @Order(4) void replaysTheDatabaseCommitOffsetCommitGap() throws Exception {
        var stopped = new CountDownLatch(1);
        listeners.stop(stopped::countDown);
        assertTrue(stopped.await(20, TimeUnit.SECONDS));
        var event = event("commit-gap");
        try {
            var receipt = post(event, TestTokens.tenant("commit-gap"));
            assertEquals(202, receipt.statusCode());
            assertEquals(EventSink.Outcome.INSERTED, sink.persist(event));
            assertFalse(isCommitted(receipt), "Consumer must not have committed the newly appended record");
            assertEquals(1, rows(event));
            // Inject the state left by a crash after DB commit but before Kafka offset commit.
            listeners.start();
            awaitCommitted(receipt);
            assertEquals(1, rows(event), "Real replay must not duplicate the stored row");
            assertEquals(EventSink.Outcome.DUPLICATE, sink.persist(event));
        } finally {
            if (!listeners.isRunning()) listeners.start();
        }
    }

    @Test @Order(5) void quarantinesPayloadCollisionAndKeepsTheFirstPayload() throws Exception {
        var original = event("collision");
        var receipt = post(original, TestTokens.tenant("collision"));
        assertEquals(202, receipt.statusCode());
        awaitCommitted(receipt);
        var collision = new Event(original.eventId(), original.tenantId(), original.eventType(), 1,
                original.occurredAt(), Map.of("platform", "changed"));
        try (var deadLetters = deadLetters()) {
            var conflictingReceipt = post(collision, TestTokens.tenant("collision"));
            assertEquals(202, conflictingReceipt.statusCode(), "202 means log acceptance, not DB validation");
            awaitDeadLetter(deadLetters, mapper.writeValueAsString(collision));
            awaitCommitted(conflictingReceipt);
            assertEquals(Map.of("platform", "linux"), sink.find("collision", original.eventId()).orElseThrow()
                    .get("attributes"));
            assertEquals(1, rows(original));
        }
    }

    @Test @Order(6) void quarantinesPoisonBeforeAdvancingItsOffset() throws Exception {
        String poison = "{unsupported-" + UUID.randomUUID() + "}";
        try (var deadLetters = deadLetters()) {
            var sent = kafka.send(TOPIC, "poison", poison).get(10, TimeUnit.SECONDS).getRecordMetadata();
            awaitDeadLetter(deadLetters, poison);
            await().atMost(Duration.ofSeconds(30)).until(() -> committed(
                    new TopicPartition(TOPIC, sent.partition())) > sent.offset());
        }
    }

    @Test @Order(7) void databaseOutageRetainsUncommittedWorkAndRecovers() throws Exception {
        var event = event("db-outage");
        POSTGRES.getDockerClient().pauseContainerCmd(POSTGRES.getContainerId()).exec();
        HttpResponse<String> receipt;
        try {
            receipt = post(event, TestTokens.tenant("db-outage"));
            assertEquals(202, receipt.statusCode(), "Kafka acknowledgement must not wait for PostgreSQL");
            // Wait beyond the configured socket/statement timeout so this exercises the retry path.
            Thread.sleep(6500);
            assertFalse(isCommitted(receipt), "DB outage must not advance the source offset");
        } finally {
            POSTGRES.getDockerClient().unpauseContainerCmd(POSTGRES.getContainerId()).exec();
        }
        awaitCommitted(receipt);
        assertEquals(1, rows(event));
    }

    @Test @Order(100) void brokerOutageCannotReturnAccepted() throws Exception {
        KAFKA.stop();
        var response = post(event("broker-outage"), TestTokens.tenant("broker-outage"));
        assertEquals(503, response.statusCode());
        assertEquals("1", response.headers().firstValue("Retry-After").orElseThrow());
    }

    private Event event(String tenant) {
        return new Event(UUID.randomUUID(), tenant, "session.started", 1,
                Instant.parse("2026-01-01T00:00:00Z"), Map.of("platform", "linux"));
    }

    private int rows(Event event) {
        return jdbc.queryForObject("SELECT count(*) FROM telemetry_events WHERE tenant_id = ? AND event_id = ?",
                Integer.class, event.tenantId(), event.eventId());
    }

    private String body(Event event) {
        return mapper.writeValueAsString(Map.of("eventId", event.eventId(), "eventType", event.eventType(),
                "schemaVersion", event.schemaVersion(), "occurredAt", event.occurredAt(),
                "attributes", event.attributes()));
    }

    private HttpResponse<String> post(Event event, String token) throws Exception {
        return request("/v1/events", body(event), token);
    }

    private HttpResponse<String> request(String path, String body, String token) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json");
        if (token != null) builder.header("Authorization", "Bearer " + token);
        return http.send(builder.POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(UUID id, String tenant) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/events/" + id))
                .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + TestTokens.tenant(tenant))
                .GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void awaitCommitted(HttpResponse<String> receipt) {
        await().atMost(Duration.ofSeconds(30)).until(() -> isCommitted(receipt));
    }

    private boolean isCommitted(HttpResponse<String> receipt) throws Exception {
        var position = mapper.readTree(receipt.body()).get("position");
        return committed(new TopicPartition(position.get("topic").asString(),
                position.get("partition").asInt())) > position.get("offset").asLong();
    }

    private long committed(TopicPartition partition) throws Exception {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(), AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000))) {
            var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata()
                    .get(6, TimeUnit.SECONDS);
            var offset = offsets.get(partition);
            return offset == null ? -1 : offset.offset();
        }
    }

    private KafkaConsumer<String, String> deadLetters() {
        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
        var partitions = List.of(new TopicPartition(DLT, 0), new TopicPartition(DLT, 1), new TopicPartition(DLT, 2));
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
        return consumer;
    }

    private void awaitDeadLetter(KafkaConsumer<String, String> consumer, String expected) {
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                if (expected.equals(record.value())) return true;
            }
            return false;
        });
    }
}
