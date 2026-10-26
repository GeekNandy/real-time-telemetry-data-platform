package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.EventSink;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ConsumerConfiguration {
    @Bean
    DefaultErrorHandler deliveryErrors(KafkaTemplate<String, String> kafka,
            @Value("${telemetry.dead-letter-topic}") String deadLetterTopic) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(deadLetterTopic, record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));
        var errors = new DefaultErrorHandler(recoverer,
                new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        errors.addNotRetryableExceptions(PersistenceConsumer.PoisonRecord.class, EventSink.Collision.class);
        errors.setCommitRecovered(true);
        return errors;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumers, DefaultErrorHandler errors) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumers);
        factory.setCommonErrorHandler(errors);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);
        return factory;
    }
}
