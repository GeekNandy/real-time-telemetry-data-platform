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
