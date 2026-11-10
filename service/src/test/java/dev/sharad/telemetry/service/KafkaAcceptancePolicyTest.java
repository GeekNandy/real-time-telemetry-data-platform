package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaAcceptancePolicyTest {
  private static final Map<String, Object> PRODUCER =
      Map.of("acks", "all", "enable.idempotence", true);
  private static final List<Node> NODES =
      List.of(
          new Node(1, "localhost", 19091),
          new Node(2, "localhost", 19092),
          new Node(3, "localhost", 19093));

  private TopicDescription topic(int replicas, int isr) {
    return new TopicDescription(
        "events",
        false,
        List.of(
            new TopicPartitionInfo(
                0, NODES.getFirst(), NODES.subList(0, replicas), NODES.subList(0, isr))));
  }

  private Config config(int minimum, boolean unclean) {
    return new Config(
        List.of(
            new ConfigEntry("min.insync.replicas", Integer.toString(minimum)),
            new ConfigEntry("unclean.leader.election.enable", Boolean.toString(unclean))));
  }

  @Test
  void acceptsTheEffectiveAllAcksPolicyAndRejectsWeakerOverrides() {
    KafkaAcceptancePolicy.validateProducer(PRODUCER);
    KafkaAcceptancePolicy.validateProducer(Map.of("acks", "-1", "enable.idempotence", true));
    for (var acks : List.of("0", "1", "all"))
      assertThrows(
          IllegalArgumentException.class,
          () ->
              KafkaAcceptancePolicy.validateProducer(
                  Map.of("acks", acks, "enable.idempotence", !acks.equals("all"))));
  }

  @Test
  void replicatedModeCannotDeclareSingleBrokerDurability() {
    var admin = new KafkaAdmin(Map.of("bootstrap.servers", "localhost:19091"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new KafkaAcceptancePolicy(admin, PRODUCER, List.of("events"), 1, 1, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> new KafkaAcceptancePolicy(admin, PRODUCER, List.of("events"), 3, 1, true));
  }

  @Test
  void checksActualReplicationIsrAndTopicPolicy() {
    KafkaAcceptancePolicy.validateTopic(topic(3, 2), config(2, false), 3, 2);
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateTopic(topic(1, 1), config(2, false), 3, 2));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateTopic(topic(3, 1), config(2, false), 3, 2));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateTopic(topic(3, 3), config(1, false), 3, 2));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateTopic(topic(3, 3), config(2, true), 3, 2));
  }

  @Test
  void verifiesEveryPartitionAndRejectsClampedMinimumIsr() {
    var mixed =
        new TopicDescription(
            "events",
            false,
            List.of(
                topic(3, 3).partitions().getFirst(),
                new TopicPartitionInfo(
                    1, NODES.getFirst(), NODES.subList(0, 1), NODES.subList(0, 1))));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateTopic(mixed, config(2, false), 3, 2));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateTopic(topic(3, 3), config(4, false), 3, 2));
  }

  private Config messageLimit(int bytes) {
    return new Config(List.of(new ConfigEntry("max.message.bytes", Integer.toString(bytes))));
  }

  @Test
  void quarantineRequiresEffectiveSourceAndDeadLetterByteBudgets() {
    var source = messageLimit(KafkaAcceptancePolicy.SOURCE_MAX_BYTES);
    var deadLetters = messageLimit(KafkaAcceptancePolicy.DEAD_LETTER_MAX_BYTES);
    KafkaAcceptancePolicy.validateRecordBudgets(source, deadLetters);
    KafkaAcceptancePolicy.validateRecordBudgets(messageLimit(32768), deadLetters);
    KafkaAcceptancePolicy.validateRecordBudgets(source, messageLimit(4 * 1024 * 1024));
    for (int invalid : new int[] {0, -1, KafkaAcceptancePolicy.SOURCE_MAX_BYTES + 1})
      assertThrows(
          IllegalStateException.class,
          () -> KafkaAcceptancePolicy.validateRecordBudgets(messageLimit(invalid), deadLetters));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateRecordBudgets(source, source));
    assertThrows(
        IllegalStateException.class,
        () ->
            KafkaAcceptancePolicy.validateRecordBudgets(
                source, messageLimit(KafkaAcceptancePolicy.DEAD_LETTER_MAX_BYTES - 1)));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateRecordBudgets(new Config(List.of()), deadLetters));
    assertThrows(
        IllegalStateException.class,
        () -> KafkaAcceptancePolicy.validateRecordBudgets(source, new Config(List.of())));
  }

  @Test
  void quarantineClonesTheProducerPolicyWithoutIncreasingTheCollectorBudget() {
    var properties =
        Map.<String, Object>of(
            "bootstrap.servers",
            "localhost:19091",
            "acks",
            "all",
            "enable.idempotence",
            true,
            "max.request.size",
            32768,
            "buffer.memory",
            16_777_216,
            "security.protocol",
            "SASL_SSL");
    var factory = new DefaultKafkaProducerFactory<String, byte[]>(properties);
    var original = new KafkaTemplate<>(factory);
    try (var transport = new ConsumerConfiguration().deadLetterTransport(original)) {
      var copied = transport.template().getProducerFactory();
      assertNotSame(factory, copied);
      assertEquals(32768, factory.getConfigurationProperties().get("max.request.size"));
      assertEquals(
          KafkaAcceptancePolicy.DEAD_LETTER_MAX_BYTES,
          copied.getConfigurationProperties().get("max.request.size"));
      for (String key :
          List.of(
              "bootstrap.servers",
              "acks",
              "enable.idempotence",
              "buffer.memory",
              "security.protocol"))
        assertEquals(properties.get(key), copied.getConfigurationProperties().get(key));
    } finally {
      original.destroy();
      factory.destroy();
    }
  }
}
