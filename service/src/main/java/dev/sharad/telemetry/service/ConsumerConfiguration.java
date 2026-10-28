package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.EventSink;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Profile("!collector")
public class ConsumerConfiguration {
  @Bean
  DefaultErrorHandler deliveryErrors(
      KafkaTemplate<String, byte[]> kafka,
      @Value("${telemetry.dead-letter-topic}") String deadLetterTopic) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafka, (record, error) -> new TopicPartition(deadLetterTopic, record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));
    var errors =
        new DefaultErrorHandler(
            recoverer, new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    errors.addNotRetryableExceptions(
        PersistenceConsumer.PoisonRecord.class, EventSink.Collision.class);
    errors.setCommitRecovered(true);
    return errors;
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
      ConsumerFactory<String, byte[]> consumers, DefaultErrorHandler errors) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
    factory.setConsumerFactory(consumers);
    factory.setCommonErrorHandler(errors);
    Object configured = consumers.getConfigurationProperties().get("max.poll.records");
    int maximum = configured == null ? 500 : Integer.parseInt(configured.toString());
    if (maximum < 1 || maximum > 256)
      throw new IllegalArgumentException("max.poll.records must be explicit and within 1..256");
    factory.setBatchListener(true);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.getContainerProperties().setSyncCommits(true);
    return factory;
  }
}
