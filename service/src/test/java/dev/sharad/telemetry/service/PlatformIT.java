package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import dev.sharad.telemetry.core.IngestionEngine;
import dev.sharad.telemetry.schema.AvroEventCodec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Real HTTP, JWT verification, Kafka, Confluent registry and PostgreSQL. Never skip for missing
 * Docker.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PlatformIT {
  static final String TOPIC = "telemetry.events.v1";
  static final String DLT = TOPIC + ".dlt";
  static final String GROUP = "telemetry-persistence-v1";
  static final Network NETWORK = Network.newNetwork();

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer("apache/kafka:4.0.0")
          .withNetwork(NETWORK)
          .withNetworkAliases("kafka")
          .withListener("kafka:19092");

  @Container
  static final GenericContainer<?> REGISTRY =
      new GenericContainer<>("confluentinc/cp-schema-registry:8.2.0")
          .withNetwork(NETWORK)
          .withExposedPorts(8081)
          .dependsOn(KAFKA)
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "registry")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
          .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
          .withStartupTimeout(Duration.ofSeconds(120));

  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:17.11-alpine")
          .withDatabaseName("telemetry")
          .withUsername("telemetry")
          .withPassword("integration-test-only");

  static final Path BUFFER = temporaryDirectory();

  private static Path temporaryDirectory() {
    try {
      return Files.createTempDirectory("telemetry-container-contract-");
    } catch (java.io.IOException failure) {
      throw new IllegalStateException(failure);
    }
  }

  @DynamicPropertySource
  static void configure(DynamicPropertyRegistry properties) {
    properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    properties.add(
        "spring.datasource.url",
        () ->
            POSTGRES.getJdbcUrl()
                + "?connectTimeout=2&socketTimeout=5&options=-c%20statement_timeout=3000");
    properties.add("spring.datasource.username", POSTGRES::getUsername);
    properties.add("spring.datasource.password", POSTGRES::getPassword);
    properties.add(
        "telemetry.schema.url",
        () -> "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(8081));
    properties.add("telemetry.schema.allow-registration", () -> true);
    properties.add("telemetry.buffer.path", BUFFER::toString);
    properties.add("telemetry.buffer.max-records", () -> 8);
    properties.add("telemetry.retention.sweep-ms", () -> 3600000);
    properties.add("telemetry.security.mode", () -> "demo");
    properties.add("telemetry.security.issuer", () -> TestTokens.ISSUER);
    properties.add("telemetry.security.demo-secret", () -> TestTokens.SECRET);
  }

  @Value("${local.server.port}")
  int port;

  @Autowired JsonMapper mapper;
  @Autowired JdbcTemplate jdbc;
  @Autowired PostgresEventSink sink;
  @Autowired KafkaTemplate<String, byte[]> kafka;
  @Autowired KafkaListenerEndpointRegistry listeners;
  @Autowired DiskBuffer buffer;
  @Autowired AvroEventCodec codec;
  @Autowired RetentionMaintenance retention;
  @Autowired IngestionEngine engine;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  @Test
  @Order(1)
  void persistsAndDeduplicatesAllAcknowledgedRetries() throws Exception {
    var event = event("tenant-a");
    assertEquals(202, post(event).statusCode());
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertEquals(1, rows(event)));
    for (int retry = 0; retry < 3; retry++) assertEquals(202, post(event).statusCode());
    awaitDrained();
    awaitAllSourceCommitted();
    assertEquals(1, rows(event));
    assertEquals(200, get(event.eventId(), "tenant-a").statusCode());
    assertEquals(404, get(event.eventId(), "tenant-b").statusCode());
  }

  @Test
  @Order(2)
  void verifiesAuthenticationScopeTenantBindingAndPayloadBounds() throws Exception {
    var event = event("validation");
    assertEquals(401, request(body(event), null).statusCode());
    String readOnly =
        TestTokens.sign(
            TestTokens.claims("validation").claim("scope", "telemetry.read").build(),
            TestTokens.SECRET);
    assertEquals(403, request(body(event), readOnly).statusCode());
    assertEquals(
        400,
        request(
                body(event).replace("\"schemaVersion\":1", "\"schemaVersion\":9"),
                TestTokens.tenant("validation"))
            .statusCode());
    assertEquals(
        400,
        request(
                body(event).replaceFirst("\\{", "{\"tenantId\":\"other\","),
                TestTokens.tenant("validation"))
            .statusCode());
    assertEquals(
        413,
        request(body(event).replace("linux", "x".repeat(20000)), TestTokens.tenant("validation"))
            .statusCode());
    assertEquals(0, rows(event));
  }

  @Test
  @Order(3)
  void reconcilesTheExactAcceptedAndStoredIdSets() throws Exception {
    var accepted = new HashSet<UUID>();
    for (int index = 0; index < 40; index++) {
      var event = event("reconciliation");
      assertEquals(202, post(event).statusCode());
      accepted.add(event.eventId());
      // Limit generator pressure; this is a correctness test, not a capacity experiment.
      if (index % 4 == 3) awaitDrained();
    }
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              var stored =
                  jdbc.query(
                      "SELECT event_id FROM telemetry_events WHERE tenant_id=?",
                      (row, index) -> row.getObject(1, UUID.class),
                      "reconciliation");
              assertEquals(accepted.size(), stored.size());
              assertEquals(accepted, new HashSet<>(stored));
            });
  }

  @Test
  @Order(4)
  void replaysTheDatabaseCommitOffsetCommitGap() throws Exception {
    awaitDrained();
    awaitAllSourceCommitted();
    var stopped = new CountDownLatch(1);
    listeners.stop(stopped::countDown);
    assertTrue(stopped.await(20, TimeUnit.SECONDS));
    var event = event("commit-gap");
    try {
      assertEquals(202, post(event).statusCode());
      awaitDrained();
      assertEquals(EventSink.Outcome.INSERTED, sink.persist(event));
      assertFalse(
          allSourceCommitted(),
          "Newly appended record must remain uncommitted while listeners are stopped");
      listeners.start();
      awaitAllSourceCommitted();
      assertEquals(1, rows(event));
      assertEquals(EventSink.Outcome.DUPLICATE, sink.persist(event));
    } finally {
      if (!listeners.isRunning()) listeners.start();
    }
  }

  @Test
  @Order(5)
  void quarantinesAConflictAndPreservesTheOriginalPayload() throws Exception {
    var original = event("collision");
    assertEquals(202, post(original).statusCode());
    awaitDrained();
    awaitAllSourceCommitted();
    var conflicting =
        new Event(
            original.eventId(),
            original.tenantId(),
            original.eventType(),
            1,
            original.occurredAt(),
            Map.of("platform", "changed"));
    try (var deadLetters = deadLetters()) {
      assertEquals(
          202,
          post(conflicting).statusCode(),
          "Acceptance precedes asynchronous collision detection");
      awaitDeadLetter(deadLetters, codec.encode(conflicting));
      awaitAllSourceCommitted();
      assertEquals(
          Map.of("platform", "linux"),
          sink.find("collision", original.eventId()).orElseThrow().get("attributes"));
      assertEquals(1, rows(original));
    }
  }

  @Test
  @Order(6)
  void quarantinesMalformedWireDataBeforeCommittingItsSourceOffset() throws Exception {
    byte[] poison =
        ("not-avro-" + UUID.randomUUID()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (var deadLetters = deadLetters()) {
      kafka.send(TOPIC, "poison", poison).get(10, TimeUnit.SECONDS);
      awaitDeadLetter(deadLetters, poison);
      awaitAllSourceCommitted();
    }
  }

  @Test
  @Order(7)
  void databaseOutagePreservesUncommittedWorkAndEventuallyStoresIt() throws Exception {
    awaitDrained();
    awaitAllSourceCommitted();
    var event = event("database-outage");
    POSTGRES.getDockerClient().pauseContainerCmd(POSTGRES.getContainerId()).exec();
    try {
      assertEquals(202, post(event).statusCode());
      awaitDrained();
      // Exceed the JDBC socket/statement timeouts and exercise the actual retry path.
      Thread.sleep(6500);
      assertEquals(503, get(event.eventId(), event.tenantId()).statusCode());
      assertFalse(allSourceCommitted());
    } finally {
      POSTGRES.getDockerClient().unpauseContainerCmd(POSTGRES.getContainerId()).exec();
    }
    awaitAllSourceCommitted();
    assertEquals(1, rows(event));
  }

  @Test
  @Order(8)
  void retentionUsesStorageTimeAndLeavesRecentRowsIntact() throws Exception {
    var old = event("retention");
    var recent = event("retention");
    assertEquals(202, post(old).statusCode());
    assertEquals(202, post(recent).statusCode());
    awaitDrained();
    awaitAllSourceCommitted();
    jdbc.update(
        "UPDATE telemetry_events SET stored_at=now()-interval '31 days' WHERE tenant_id=? AND"
            + " event_id=?",
        old.tenantId(),
        old.eventId());
    assertEquals(1, retention.purge());
    assertEquals(0, rows(old));
    assertEquals(1, rows(recent));
  }

  @Test
  @Order(9)
  void brokerOutageUsesTheDurableBudgetAndRecoversWithoutLosingAcceptedIds() throws Exception {
    awaitDrained();
    awaitAllSourceCommitted();
    var accepted = new HashSet<UUID>();
    KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
    try {
      for (int index = 0; index < 8; index++) {
        var event = event("broker-outage");
        assertEquals(202, post(event).statusCode());
        accepted.add(event.eventId());
      }
      assertEquals(8, buffer.snapshot().records());
      var rejected = post(event("broker-outage"));
      assertEquals(503, rejected.statusCode());
      assertEquals("1", rejected.headers().firstValue("Retry-After").orElseThrow());
    } finally {
      KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }
    await()
        .atMost(Duration.ofSeconds(40))
        .untilAsserted(
            () -> {
              var stored =
                  jdbc.query(
                      "SELECT event_id FROM telemetry_events WHERE tenant_id=?",
                      (row, index) -> row.getObject(1, UUID.class),
                      "broker-outage");
              assertEquals(accepted.size(), stored.size());
              assertEquals(accepted, new HashSet<>(stored));
            });
    awaitDrained();
    awaitAllSourceCommitted();
  }

  @Test
  @Order(100)
  void drainingRejectsNewWorkWithoutInventingAcceptance() throws Exception {
    engine.beginDrain().toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals(503, post(event("draining")).statusCode());
  }

  private Event event(String tenant) {
    return new Event(
        UUID.randomUUID(),
        tenant,
        "session.started",
        1,
        Instant.parse("2026-01-01T00:00:00Z"),
        Map.of("platform", "linux"));
  }

  private int rows(Event event) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM telemetry_events WHERE tenant_id=? AND event_id=?",
        Integer.class,
        event.tenantId(),
        event.eventId());
  }

  private String body(Event event) {
    return mapper.writeValueAsString(
        Map.of(
            "eventId",
            event.eventId(),
            "eventType",
            event.eventType(),
            "schemaVersion",
            event.schemaVersion(),
            "occurredAt",
            event.occurredAt(),
            "attributes",
            event.attributes()));
  }

  private HttpResponse<String> post(Event event) throws Exception {
    return request(body(event), TestTokens.tenant(event.tenantId()));
  }

  private HttpResponse<String> request(String body, String token) throws Exception {
    var builder =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/events"))
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/json");
    if (token != null) builder.header("Authorization", "Bearer " + token);
    return http.send(
        builder.POST(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(UUID id, String tenant) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/events/" + id))
            .timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer " + TestTokens.tenant(tenant))
            .GET()
            .build();
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private void awaitDrained() {
    await().atMost(Duration.ofSeconds(30)).until(() -> buffer.snapshot().records() == 0);
  }

  private void awaitAllSourceCommitted() {
    await().atMost(Duration.ofSeconds(30)).until(this::allSourceCommitted);
  }

  private boolean allSourceCommitted() throws Exception {
    try (var admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                10000,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                3000))) {
      var requested =
          Map.of(
              new TopicPartition(TOPIC, 0),
              OffsetSpec.latest(),
              new TopicPartition(TOPIC, 1),
              OffsetSpec.latest(),
              new TopicPartition(TOPIC, 2),
              OffsetSpec.latest());
      var ends = admin.listOffsets(requested).all().get(10, TimeUnit.SECONDS);
      var committed =
          admin
              .listConsumerGroupOffsets(GROUP)
              .partitionsToOffsetAndMetadata()
              .get(10, TimeUnit.SECONDS);
      return ends.entrySet().stream()
          .allMatch(
              entry ->
                  entry.getValue().offset() == 0
                      || (committed.containsKey(entry.getKey())
                          && committed.get(entry.getKey()).offset() >= entry.getValue().offset()));
    }
  }

  private KafkaConsumer<String, byte[]> deadLetters() {
    var consumer =
        new KafkaConsumer<String, byte[]>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "dlt-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()));
    var partitions =
        List.of(new TopicPartition(DLT, 0), new TopicPartition(DLT, 1), new TopicPartition(DLT, 2));
    consumer.assign(partitions);
    consumer.seekToEnd(partitions);
    partitions.forEach(consumer::position);
    return consumer;
  }

  private void awaitDeadLetter(KafkaConsumer<String, byte[]> consumer, byte[] expected) {
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              for (var record : consumer.poll(Duration.ofMillis(200)))
                if (Arrays.equals(expected, record.value())) return true;
              return false;
            });
  }
}
