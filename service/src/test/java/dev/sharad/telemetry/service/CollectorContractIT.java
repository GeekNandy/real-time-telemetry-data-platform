package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import dev.sharad.telemetry.schema.AvroEventCodec;
import dev.sharad.telemetry.schema.RegistryClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

/**
 * Exercises the packaged collector over HTTP, a real volume, Kafka and Schema Registry. PostgreSQL
 * is not used.
 */
class CollectorContractIT {
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private Process collector;
  private int port;
  private Path work;

  @Test
  void verifiesIdentityTracePropagationBufferLimitsAndReplayAfterSigkill() throws Exception {
    try (var infrastructure = new LocalBrokerRegistry()) {
      work = Files.createTempDirectory("telemetry-collector-contract-");
      try {
        start(infrastructure);
        String token = TestTokens.tenant("collector");
        try (var consumer = consumer(infrastructure, false)) {
          UUID first = UUID.randomUUID();
          var response =
              post(first, token, "00-11111111111111111111111111111111-2222222222222222-01");
          assertEquals(202, response.statusCode(), response.body());
          var codec =
              new AvroEventCodec(new RegistryClient(infrastructure.registryUrl(), false, ""));
          var observed = new java.util.concurrent.atomic.AtomicBoolean();
          await()
              .atMost(Duration.ofSeconds(30))
              .until(
                  () -> {
                    for (var record : consumer.poll(Duration.ofMillis(200))) {
                      if (codec.decode(record.value()).eventId().equals(first)) {
                        String trace =
                            new String(
                                record.headers().lastHeader("traceparent").value(),
                                StandardCharsets.UTF_8);
                        assertTrue(trace.startsWith("00-11111111111111111111111111111111-"));
                        observed.set(true);
                      }
                    }
                    return observed.get();
                  });
          await().atMost(Duration.ofSeconds(10)).until(() -> pendingFiles() == 0);
          assertEquals(401, post(UUID.randomUUID(), null, null).statusCode());
          String readOnly =
              TestTokens.sign(
                  TestTokens.claims("collector").claim("scope", "telemetry.read").build(),
                  TestTokens.SECRET);
          assertEquals(403, post(UUID.randomUUID(), readOnly, null).statusCode());
          String spoof = body(UUID.randomUUID()).replaceFirst("\\{", "{\"tenantId\":\"other\",");
          assertEquals(400, request(spoof, token, null).statusCode());
          assertEquals(
              400,
              request(
                      body(UUID.randomUUID()).replace("\"schemaVersion\":1", "\"schemaVersion\":9"),
                      token,
                      null)
                  .statusCode());
          assertEquals(
              413,
              request(body(UUID.randomUUID()).replace("linux", "x".repeat(20000)), token, null)
                  .statusCode());

          infrastructure.stopBroker();
          Set<UUID> unconfirmed = new HashSet<>();
          for (int i = 0; i < 8; i++) {
            UUID id = UUID.randomUUID();
            var responseWithoutAcknowledgement = post(id, token, null);
            assertEquals(
                503,
                responseWithoutAcknowledgement.statusCode(),
                responseWithoutAcknowledgement.body());
            assertEquals(
                "1",
                responseWithoutAcknowledgement.headers().firstValue("Retry-After").orElseThrow());
            unconfirmed.add(id);
          }
          assertEquals(8, pendingFiles());
          assertEquals(
              503,
              post(UUID.randomUUID(), token, null).statusCode(),
              "A full disk queue cannot acknowledge more data");
          collector.destroyForcibly();
          assertTrue(collector.waitFor(10, TimeUnit.SECONDS));
          consumer.close();
          infrastructure.restartBroker();
          infrastructure.stopRegistry();
          infrastructure.startRegistry();
          start(infrastructure); // Reopen the exact persistent volume after an ungraceful JVM
          // termination.
          Set<UUID> replayed = new HashSet<>();
          try (var recoveryConsumer = consumer(infrastructure, true)) {
            await()
                .atMost(Duration.ofSeconds(40))
                .until(
                    () -> {
                      for (var record : recoveryConsumer.poll(Duration.ofMillis(200))) {
                        UUID id = codec.decode(record.value()).eventId();
                        if (unconfirmed.contains(id)) replayed.add(id);
                      }
                      return replayed.equals(unconfirmed);
                    });
          }
          await().atMost(Duration.ofSeconds(10)).until(() -> pendingFiles() == 0);
          assertEquals(unconfirmed, replayed);
        }
      } finally {
        stopCollector();
      }
    }
  }

  private void start(LocalBrokerRegistry infrastructure) throws Exception {
    port = LocalBrokerRegistry.freePort();
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    Path jar = Path.of("target/telemetry-service-0.1.0-SNAPSHOT.jar").toAbsolutePath();
    collector =
        new ProcessBuilder(
                java,
                "-Xms128m",
                "-Xmx512m",
                "-jar",
                jar.toString(),
                "--spring.profiles.active=collector",
                "--server.port=" + port,
                "--spring.kafka.bootstrap-servers=" + infrastructure.brokers(),
                "--telemetry.schema.url=" + infrastructure.registryUrl(),
                "--telemetry.schema.allow-registration=true",
                "--telemetry.buffer.path=" + work.resolve("wal"),
                "--telemetry.buffer.max-records=8",
                "--telemetry.buffer.max-bytes=1048576",
                "--telemetry.acceptance-deadline-ms=3000",
                "--telemetry.security.mode=demo",
                "--telemetry.security.issuer=" + TestTokens.ISSUER,
                "--telemetry.security.demo-secret=" + TestTokens.SECRET,
                "--logging.level.org.apache.kafka=WARN")
            .redirectErrorStream(true)
            .redirectOutput(
                ProcessBuilder.Redirect.appendTo(work.resolve("collector.log").toFile()))
            .start();
    await()
        .atMost(Duration.ofSeconds(60))
        .ignoreExceptionsInstanceOf(java.io.IOException.class)
        .until(
            () -> {
              if (!collector.isAlive())
                throw new IllegalStateException(
                    "Collector exited; inspect " + work.resolve("collector.log"));
              return http.send(
                          HttpRequest.newBuilder(
                                  URI.create(
                                      "http://127.0.0.1:" + port + "/actuator/health/readiness"))
                              .timeout(Duration.ofSeconds(2))
                              .GET()
                              .build(),
                          HttpResponse.BodyHandlers.discarding())
                      .statusCode()
                  == 200;
            });
  }

  private void stopCollector() throws Exception {
    if (collector != null) {
      collector.destroy();
      if (!collector.waitFor(15, TimeUnit.SECONDS)) collector.destroyForcibly();
    }
  }

  private long pendingFiles() throws Exception {
    try (var files = Files.list(work.resolve("wal"))) {
      return files
          .filter(p -> p.toString().endsWith(".evt") || p.toString().endsWith(".grp"))
          .count();
    }
  }

  private KafkaConsumer<String, byte[]> consumer(
      LocalBrokerRegistry infrastructure, boolean fromStart) {
    var consumer =
        new KafkaConsumer<String, byte[]>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, infrastructure.brokers(),
                ConsumerConfig.GROUP_ID_CONFIG, "collector-contract-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName()));
    List<TopicPartition> partitions =
        List.of(
            new TopicPartition("telemetry.events.v1", 0),
            new TopicPartition("telemetry.events.v1", 1),
            new TopicPartition("telemetry.events.v1", 2));
    consumer.assign(partitions);
    if (fromStart) consumer.seekToBeginning(partitions);
    else consumer.seekToEnd(partitions);
    partitions.forEach(consumer::position);
    return consumer;
  }

  private HttpResponse<String> post(UUID id, String token, String trace) throws Exception {
    return request(body(id), token, trace);
  }

  private String body(UUID id) {
    return "{\"eventId\":\""
        + id
        + "\",\"eventType\":\"session.started\",\"schemaVersion\":1,"
        + "\"occurredAt\":\"2026-01-01T00:00:00Z\",\"attributes\":{\"platform\":\"linux\"}}";
  }

  private HttpResponse<String> request(String body, String token, String trace) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/events"))
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/json");
    if (token != null) request.header("Authorization", "Bearer " + token);
    if (trace != null) request.header("traceparent", trace);
    return http.send(
        request.POST(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
