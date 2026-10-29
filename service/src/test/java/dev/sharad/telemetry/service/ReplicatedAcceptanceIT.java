package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real HTTP processes, three Kafka data brokers, registry and PostgreSQL; no skipped Docker path.
 */
@Testcontainers
class ReplicatedAcceptanceIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:17.11-alpine")
          .withDatabaseName("telemetry")
          .withUsername("telemetry")
          .withPassword("integration-test-only");

  @TempDir Path directory;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private Process collector;
  private Process worker;
  private int collectorPort;

  @Test
  void preservesReplicatedAcceptanceAcrossVolumeLossIsrFailureCancellationAndReplay()
      throws Exception {
    try (var infrastructure = new LocalBrokerRegistry(3);
        var admin =
            Admin.create(
                Map.of(
                    "bootstrap.servers",
                    infrastructure.brokers(),
                    "default.api.timeout.ms",
                    10_000,
                    "request.timeout.ms",
                    3000))) {
      try {
        assertReplicatedPolicy(admin);
        startCollector(infrastructure);
        var accepted = new HashSet<UUID>();
        var burst = new java.util.ArrayList<UUID>();
        var replies =
            new java.util.ArrayList<java.util.concurrent.CompletableFuture<HttpResponse<String>>>();
        for (int index = 0; index < 8; index++) {
          var id = UUID.randomUUID();
          burst.add(id);
          replies.add(http.sendAsync(request(id), HttpResponse.BodyHandlers.ofString()));
        }
        for (int index = 0; index < burst.size(); index++) {
          var response = replies.get(index).get(20, TimeUnit.SECONDS);
          assertEquals(202, response.statusCode(), response.body());
          assertTrue(
              response.body().contains("\"stream\":\"telemetry.events.v1\""), response.body());
          assertTrue(response.body().contains(burst.get(index).toString()), response.body());
          accepted.add(burst.get(index));
        }
        replies.clear();
        for (var id : burst)
          replies.add(http.sendAsync(request(id), HttpResponse.BodyHandlers.ofString()));
        for (var reply : replies) assertEquals(202, reply.get(20, TimeUnit.SECONDS).statusCode());
        // No worker has run: the database cannot mask a lost collector-only record.
        killCollector();
        removeVolume(directory.resolve("wal"));
        assertFalse(Files.exists(directory.resolve("wal")));
        int stopped = partitionLeader(admin);
        infrastructure.broker.brokers().get(stopped).shutdown();
        awaitIsr(admin, 2);
        startWorker(infrastructure);
        startCollector(infrastructure); // Brand-new volume; accepted records must come from Kafka.
        awaitRows(accepted);

        int another =
            infrastructure.broker.brokers().keySet().stream()
                .filter(id -> id != stopped)
                .findFirst()
                .orElseThrow();
        infrastructure.broker.brokers().get(another).shutdown();
        awaitIsr(admin, 1);
        UUID timedOut = UUID.randomUUID();
        var unconfirmed = post(timedOut);
        assertEquals(503, unconfirmed.statusCode(), unconfirmed.body());
        assertEquals("1", unconfirmed.headers().firstValue("Retry-After").orElseThrow());
        UUID cancelled = UUID.randomUUID();
        var response = http.sendAsync(request(cancelled), HttpResponse.BodyHandlers.ofString());
        await().atMost(Duration.ofSeconds(3)).until(() -> pendingFiles() == 2);
        assertFalse(response.isDone());
        assertTrue(response.cancel(true));
        assertEquals(2, pendingFiles(), "Cancelling HTTP must preserve already-appended work");

        infrastructure.broker.brokers().get(stopped).startup();
        infrastructure.broker.brokers().get(another).startup();
        infrastructure.broker.waitForReadyBrokers();
        awaitIsr(admin, 3);
        await().atMost(Duration.ofSeconds(40)).until(() -> pendingFiles() == 0);
        for (var id : List.of(timedOut, cancelled)) {
          assertEquals(202, post(id).statusCode());
          accepted.add(id);
        }
        awaitRows(accepted); // Every retry still has exactly one downstream row.

        // Force the ack/checkpoint crash window with an actual filesystem write failure.
        Path blocked = directory.resolve("wal/checkpoint.tmp");
        Files.createDirectory(blocked);
        Files.writeString(blocked.resolve("blocker"), "synthetic checkpoint failure");
        UUID checkpointGap = UUID.randomUUID();
        assertEquals(503, post(checkpointGap).statusCode());
        assertEquals(1, pendingFiles());
        killCollector();
        Files.delete(blocked.resolve("blocker"));
        Files.delete(blocked);
        startCollector(
            infrastructure); // The old checkpoint forces replay of the broker-confirmed ID.
        await().atMost(Duration.ofSeconds(30)).until(() -> pendingFiles() == 0);
        assertEquals(202, post(checkpointGap).statusCode());
        accepted.add(checkpointGap);
        awaitRows(accepted);
      } finally {
        stop(collector);
        stop(worker);
      }
    }
  }

  private void assertReplicatedPolicy(Admin admin) throws Exception {
    var resource = new ConfigResource(ConfigResource.Type.TOPIC, "telemetry.events.v1");
    var config =
        admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
    assertEquals("2", config.get("min.insync.replicas").value());
    for (var partition : description(admin).partitions())
      assertEquals(3, partition.replicas().size());
    awaitIsr(admin, 3);
  }

  private org.apache.kafka.clients.admin.TopicDescription description(Admin admin)
      throws Exception {
    return admin
        .describeTopics(List.of("telemetry.events.v1"))
        .allTopicNames()
        .get(10, TimeUnit.SECONDS)
        .get("telemetry.events.v1");
  }

  private int partitionLeader(Admin admin) throws Exception {
    return description(admin).partitions().getFirst().leader().id();
  }

  private void awaitIsr(Admin admin, int size) {
    await()
        .atMost(Duration.ofSeconds(40))
        .ignoreExceptions()
        .until(
            () ->
                description(admin).partitions().stream()
                    .allMatch(partition -> partition.isr().size() == size));
  }

  private void startCollector(LocalBrokerRegistry infrastructure) throws Exception {
    collectorPort = LocalBrokerRegistry.freePort();
    collector = start(infrastructure, "collector", collectorPort);
  }

  private void startWorker(LocalBrokerRegistry infrastructure) throws Exception {
    worker = start(infrastructure, "worker", LocalBrokerRegistry.freePort());
  }

  private Process start(LocalBrokerRegistry infrastructure, String role, int port)
      throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin/java").toString();
    Path jar = Path.of("target/telemetry-service-0.1.0-SNAPSHOT.jar").toAbsolutePath();
    var builder =
        new ProcessBuilder(
                java,
                "-Xms128m",
                "-Xmx512m",
                "-XX:ActiveProcessorCount=2",
                "-jar",
                jar.toString(),
                "--spring.profiles.active=" + role + ",replicated",
                "--server.port=" + port,
                "--spring.kafka.bootstrap-servers=" + infrastructure.brokers(),
                "--telemetry.schema.url=" + infrastructure.registryUrl(),
                "--telemetry.schema.allow-registration=true",
                "--telemetry.buffer.path=" + directory.resolve("wal"),
                "--telemetry.buffer.max-records=16",
                "--telemetry.acceptance-deadline-ms=5000",
                "--telemetry.security.mode=demo",
                "--telemetry.security.issuer=" + TestTokens.ISSUER,
                "--logging.level.org.apache.kafka=WARN")
            .redirectErrorStream(true)
            .redirectOutput(
                ProcessBuilder.Redirect.appendTo(directory.resolve(role + ".log").toFile()));
    builder.environment().put("DEMO_JWT_SECRET", TestTokens.SECRET);
    builder
        .environment()
        .put(
            "DATABASE_URL",
            POSTGRES.getJdbcUrl()
                + "?connectTimeout=2&socketTimeout=5&options=-c%20statement_timeout=3000");
    builder.environment().put("DATABASE_USER", POSTGRES.getUsername());
    builder.environment().put("DATABASE_PASSWORD", POSTGRES.getPassword());
    var process = builder.start();
    try {
      await()
          .atMost(Duration.ofSeconds(60))
          .ignoreExceptionsInstanceOf(java.io.IOException.class)
          .until(
              () -> {
                if (!process.isAlive())
                  throw new IllegalStateException(
                      "Packaged " + role + " exited; inspect test logs in " + directory);
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
      return process;
    } catch (Exception failure) {
      stop(process);
      throw failure;
    }
  }

  private HttpRequest request(UUID id) throws Exception {
    String body =
        "{\"eventId\":\""
            + id
            + "\",\"eventType\":\"session.started\",\"schemaVersion\":1,"
            + "\"occurredAt\":\"2026-01-01T00:00:00Z\",\"attributes\":{\"platform\":\"replicated-test\"}}";
    return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + collectorPort + "/v1/events"))
        .timeout(Duration.ofSeconds(15))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + TestTokens.tenant("replicated-loss"))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
  }

  private HttpResponse<String> post(UUID id) throws Exception {
    return http.send(request(id), HttpResponse.BodyHandlers.ofString());
  }

  private long pendingFiles() throws Exception {
    try (var paths = Files.list(directory.resolve("wal"))) {
      return paths
          .filter(path -> path.toString().endsWith(".evt") || path.toString().endsWith(".grp"))
          .count();
    }
  }

  private void awaitRows(Set<UUID> accepted) {
    await()
        .atMost(Duration.ofSeconds(40))
        .untilAsserted(
            () -> {
              var stored = new HashSet<UUID>();
              int count = 0;
              try (var connection =
                      DriverManager.getConnection(
                          POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                  var query =
                      connection.prepareStatement(
                          "SELECT event_id FROM telemetry_events WHERE tenant_id=?")) {
                query.setString(1, "replicated-loss");
                try (var rows = query.executeQuery()) {
                  while (rows.next()) {
                    stored.add(rows.getObject(1, UUID.class));
                    count++;
                  }
                }
              }
              assertEquals(accepted, stored);
              assertEquals(accepted.size(), count);
            });
  }

  private void killCollector() throws Exception {
    collector.destroyForcibly();
    assertTrue(collector.waitFor(10, TimeUnit.SECONDS));
  }

  private static void stop(Process process) throws Exception {
    if (process == null) return;
    process.destroy();
    if (!process.waitFor(15, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
    }
  }

  private static void removeVolume(Path path) throws Exception {
    try (var paths = Files.walk(path)) {
      for (var entry : paths.sorted(java.util.Comparator.reverseOrder()).toList())
        Files.delete(entry);
    }
  }
}
