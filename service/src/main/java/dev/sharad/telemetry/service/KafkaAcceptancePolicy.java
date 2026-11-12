package dev.sharad.telemetry.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Verify the effective producer and broker policy before starting the collector and HTTP server.
 */
final class KafkaAcceptancePolicy implements SmartLifecycle {
  // Match Kafka's default source batch limit; quarantine adds provenance and exception headers.
  static final int SOURCE_MAX_BYTES = 1_048_588;
  static final int DEAD_LETTER_MAX_BYTES = 2_097_152;
  private final KafkaAdmin kafka;
  private final Map<String, Object> producer;
  private final List<String> topics;
  private final int replicas;
  private final int minimum;
  private volatile boolean running;

  KafkaAcceptancePolicy(
      KafkaAdmin kafka,
      Map<String, Object> producer,
      List<String> topics,
      int replicas,
      int minimum,
      boolean replicated) {
    if (replicas < 1
        || minimum < 1
        || minimum > replicas
        || (replicated && (replicas < 3 || minimum < 2)))
      throw new IllegalArgumentException("Replicated acceptance requires RF >= 3 and minISR >= 2");
    this.kafka = kafka;
    this.producer = producer;
    this.topics = List.copyOf(topics);
    if (this.topics.size() != 2 || this.topics.getFirst().equals(this.topics.getLast()))
      throw new IllegalArgumentException("Require distinct source and dead-letter topics");
    this.replicas = replicas;
    this.minimum = minimum;
  }

  static void validateProducer(Map<String, Object> producer) {
    String acks = String.valueOf(producer.get(ProducerConfig.ACKS_CONFIG));
    if (!(acks.equals("all") || acks.equals("-1"))
        || !Boolean.parseBoolean(
            String.valueOf(producer.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))))
      throw new IllegalArgumentException("Acceptance requires acks=all and producer idempotence");
  }

  static void validateTopic(TopicDescription topic, Config config, int replicas, int minimum) {
    var minIsr = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    var clean = config.get(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
    if (minIsr == null
        || Integer.parseInt(minIsr.value()) < minimum
        || clean == null
        || !"false".equals(clean.value())
        || topic.partitions().isEmpty())
      throw new IllegalStateException(
          "Topic acceptance policy does not match the declared guarantee");
    for (var partition : topic.partitions())
      if (partition.replicas().size() < replicas
          || partition.isr().size() < minimum
          || Integer.parseInt(minIsr.value()) > partition.replicas().size())
        throw new IllegalStateException("Topic replication or ISR is below the declared guarantee");
  }

  static void validateRecordBudgets(Config source, Config deadLetters) {
    var sourceLimit = source.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
    var deadLetterLimit = deadLetters.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
    if (sourceLimit == null || deadLetterLimit == null)
      throw new IllegalStateException("Cannot verify source and dead-letter message byte limits");
    int sourceBytes = Integer.parseInt(sourceLimit.value());
    int deadLetterBytes = Integer.parseInt(deadLetterLimit.value());
    if (sourceBytes < 1
        || sourceBytes > SOURCE_MAX_BYTES
        || deadLetterBytes < DEAD_LETTER_MAX_BYTES)
      throw new IllegalStateException(
          "Require source max.message.bytes <= "
              + SOURCE_MAX_BYTES
              + " and dead-letter max.message.bytes >= "
              + DEAD_LETTER_MAX_BYTES);
  }

  @Override
  public void start() {
    validateProducer(producer);
    var properties = new HashMap<String, Object>(kafka.getConfigurationProperties());
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
    try (var admin = Admin.create(properties)) {
      var descriptions = admin.describeTopics(topics).allTopicNames().get(10, TimeUnit.SECONDS);
      var resources =
          topics.stream().map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).toList();
      var configs = admin.describeConfigs(resources).all().get(10, TimeUnit.SECONDS);
      for (var resource : resources)
        validateTopic(descriptions.get(resource.name()), configs.get(resource), replicas, minimum);
      validateRecordBudgets(configs.get(resources.getFirst()), configs.get(resources.getLast()));
      running = true;
    } catch (Exception failure) {
      throw new IllegalStateException("Cannot verify Kafka acceptance policy", failure);
    }
  }

  @Override
  public int getPhase() {
    return -1000;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void stop() {
    running = false;
  }
}
