package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

/**
 * Real KRaft broker and a separate Confluent Schema Registry JVM. No database or service doubles.
 */
final class LocalBrokerRegistry implements AutoCloseable {
  final KafkaClusterTestKit broker;
  final Path directory;
  final int registryPort;
  private final Path properties;
  private final int replicas;
  private final int minimum;
  private Process registry;

  LocalBrokerRegistry() throws Exception {
    this(1);
  }

  LocalBrokerRegistry(int brokers) throws Exception {
    if (brokers != 1 && brokers != 3) throw new IllegalArgumentException("Reference broker count");
    replicas = brokers;
    minimum = brokers == 3 ? 2 : 1;
    directory = Files.createTempDirectory("telemetry-real-registry-");
    registryPort = freePort();
    var nodes =
        new TestKitNodes.Builder()
            .setCombined(false)
            .setNumBrokerNodes(brokers)
            .setNumControllerNodes(1)
            .setBootstrapMetadataVersion(
                org.apache.kafka.server.common.MetadataVersion.latestProduction())
            .build();
    broker =
        new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("offsets.topic.replication.factor", Integer.toString(replicas))
            .setConfigProp("transaction.state.log.replication.factor", Integer.toString(replicas))
            .setConfigProp("transaction.state.log.min.isr", Integer.toString(minimum))
            .setConfigProp("min.insync.replicas", Integer.toString(minimum))
            .setConfigProp("unclean.leader.election.enable", "false")
            .setConfigProp("replica.lag.time.max.ms", "3000")
            .setConfigProp("group.initial.rebalance.delay.ms", "0")
            .setConfigProp("controlled.shutdown.enable", "false")
            .build();
    properties = directory.resolve("registry.properties");
    try {
      broker.format();
      broker.startup();
      broker.waitForReadyBrokers();
      try (var admin = Admin.create(Map.of("bootstrap.servers", broker.bootstrapServers()))) {
        admin
            .createTopics(
                java.util.List.of(
                    new NewTopic("telemetry.events.v1", 3, (short) replicas),
                    new NewTopic("telemetry.events.v1.dlt", 3, (short) replicas)))
            .all()
            .get(20, TimeUnit.SECONDS);
      }
      startRegistry();
    } catch (Exception failure) {
      broker.close();
      throw failure;
    }
  }

  URI registryUrl() {
    return URI.create("http://127.0.0.1:" + registryPort);
  }

  String brokers() {
    return broker.bootstrapServers();
  }

  void stopRegistry() throws Exception {
    if (registry != null) {
      registry.destroyForcibly();
      registry.waitFor(10, TimeUnit.SECONDS);
    }
  }

  void startRegistry() throws Exception {
    Files.writeString(
        properties,
        "listeners=http://127.0.0.1:"
            + registryPort
            + "\nhost.name=127.0.0.1\n"
            + "kafkastore.bootstrap.servers=PLAINTEXT://"
            + broker.bootstrapServers()
            + "\n"
            + "kafkastore.topic.replication.factor="
            + replicas
            + "\n"
            + "schema.compatibility.level=FULL_TRANSITIVE\n");
    Path classpath =
        Path.of(
                System.getProperty(
                    "registry.classpath.file",
                    "../tools/schema-registry-runtime/target/classpath.txt"))
            .toAbsolutePath();
    if (!Files.exists(classpath))
      throw new IllegalStateException(
          "Build tools/schema-registry-runtime classpath before integration execution");
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    registry =
        new ProcessBuilder(
                java,
                "-Xms128m",
                "-Xmx512m",
                "-cp",
                Files.readString(classpath).trim(),
                "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain",
                properties.toString())
            .redirectErrorStream(true)
            .redirectOutput(directory.resolve("registry.log").toFile())
            .start();
    var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    await()
        .atMost(Duration.ofSeconds(60))
        .ignoreExceptionsInstanceOf(java.io.IOException.class)
        .until(
            () -> {
              if (!registry.isAlive())
                throw new IllegalStateException(
                    "Registry exited; inspect " + directory.resolve("registry.log"));
              return client
                      .send(
                          HttpRequest.newBuilder(registryUrl().resolve("/subjects"))
                              .timeout(Duration.ofSeconds(2))
                              .GET()
                              .build(),
                          HttpResponse.BodyHandlers.discarding())
                      .statusCode()
                  == 200;
            });
  }

  void stopBroker() {
    broker.brokers().values().forEach(kafka.server.BrokerServer::shutdown);
  }

  void restartBroker() throws Exception {
    broker.brokers().values().forEach(kafka.server.BrokerServer::startup);
    broker.waitForReadyBrokers();
  }

  @Override
  public void close() throws Exception {
    try {
      stopRegistry();
    } finally {
      broker.close();
    }
  }

  static int freePort() throws IOException {
    try (var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
