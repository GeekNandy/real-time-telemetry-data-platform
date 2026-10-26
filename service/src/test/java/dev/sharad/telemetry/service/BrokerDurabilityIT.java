package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.junit.jupiter.api.Test;

class BrokerDurabilityIT {
  @Test
  void electsAnotherLeaderAndRefusesAcknowledgementBelowMinimumIsr() throws Exception {
    String topic = "replicated-" + UUID.randomUUID();
    var nodes =
        new TestKitNodes.Builder()
            .setCombined(false)
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setBootstrapMetadataVersion(
                org.apache.kafka.server.common.MetadataVersion.latestProduction())
            .build();
    var cluster =
        new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("offsets.topic.replication.factor", "3")
            .setConfigProp("default.replication.factor", "3")
            .setConfigProp("min.insync.replicas", "2")
            .setConfigProp("unclean.leader.election.enable", "false")
            .setConfigProp("replica.lag.time.max.ms", "3000")
            .setConfigProp("controlled.shutdown.enable", "false")
            .build();
    cluster.format();
    cluster.startup();
    cluster.waitForReadyBrokers();
    try (var admin =
            Admin.create(
                Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    cluster.bootstrapServers(),
                    AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                    15000,
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    3000));
        var producer =
            new KafkaProducer<String, String>(
                Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    cluster.bootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName(),
                    ProducerConfig.ACKS_CONFIG,
                    "all",
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    true,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    1000,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                    4000,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG,
                    2000))) {
      var policy = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      admin
          .createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 3)))
          .all()
          .get(20, TimeUnit.SECONDS);
      admin
          .incrementalAlterConfigs(
              Map.of(
                  policy,
                  java.util.List.of(
                      new AlterConfigOp(
                          new ConfigEntry("min.insync.replicas", "2"), AlterConfigOp.OpType.SET),
                      new AlterConfigOp(
                          new ConfigEntry("unclean.leader.election.enable", "false"),
                          AlterConfigOp.OpType.SET))))
          .all()
          .get(20, TimeUnit.SECONDS);
      var initial =
          admin
              .describeTopics(java.util.List.of(topic))
              .allTopicNames()
              .get()
              .get(topic)
              .partitions()
              .getFirst();
      assertEquals(3, initial.replicas().size());
      producer
          .send(new ProducerRecord<>(topic, "first", "acknowledged-before-failure"))
          .get(10, TimeUnit.SECONDS);
      int leader = initial.leader().id();
      cluster.brokers().get(leader).shutdown();
      await()
          .atMost(Duration.ofSeconds(30))
          .ignoreExceptions()
          .until(
              () -> {
                var partition =
                    admin
                        .describeTopics(java.util.List.of(topic))
                        .allTopicNames()
                        .get()
                        .get(topic)
                        .partitions()
                        .getFirst();
                return partition.leader() != null
                    && partition.leader().id() != leader
                    && partition.isr().size() == 2;
              });
      producer
          .send(new ProducerRecord<>(topic, "after-election", "accepted-with-two-isr"))
          .get(10, TimeUnit.SECONDS);
      int another =
          cluster.brokers().keySet().stream().filter(id -> id != leader).findFirst().orElseThrow();
      cluster.brokers().get(another).shutdown();
      await()
          .atMost(Duration.ofSeconds(30))
          .ignoreExceptions()
          .until(
              () ->
                  admin
                          .describeTopics(java.util.List.of(topic))
                          .allTopicNames()
                          .get()
                          .get(topic)
                          .partitions()
                          .getFirst()
                          .isr()
                          .size()
                      == 1);
      assertThrows(
          ExecutionException.class,
          () ->
              producer
                  .send(new ProducerRecord<>(topic, "below-isr", "unconfirmed"))
                  .get(10, TimeUnit.SECONDS));
    } finally {
      cluster.close();
    }
  }
}
