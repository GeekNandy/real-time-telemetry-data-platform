package dev.sharad.telemetry.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.EventSink;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.ProducerListener;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Real Kafka recovery must preserve large poison data and unblock its valid partition suffix. */
@Testcontainers
class DeadLetterBoundsIT {
  private static final class ClosingMeters extends SimpleMeterRegistry implements AutoCloseable {}

  @Configuration(proxyBeanMethods = false)
  static class ProducerPrivacyConfiguration {
    @Bean
    ProducerListener<Object, Object> kafkaProducerListener() {
      return new RuntimeConfiguration().kafkaProducerListener();
    }
  }

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

  @Test
  void failedCollectorAndQuarantineSendsLogNoRecordContents() throws Exception {
    String topic = "producer-privacy-" + UUID.randomUUID();
    String privateKey = "synthetic-private-tenant:event-identity";
    String privatePayload = "synthetic-private-event-payload";
    byte[] oversized = new byte[KafkaAcceptancePolicy.DEAD_LETTER_MAX_BYTES + 1];
    System.arraycopy(
        privatePayload.getBytes(StandardCharsets.UTF_8), 0, oversized, 0, privatePayload.length());
    try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(
              List.of(new RuntimeConfiguration().deadLetterTopic(topic, 1, 1, 604_800_000)))
          .all()
          .get(20, TimeUnit.SECONDS);
    }
    var logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    var captured = new ListAppender<ILoggingEvent>();
    captured.setContext(logger.getLoggerContext());
    captured.start();
    logger.addAppender(captured);
    try {
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
          .withUserConfiguration(ProducerPrivacyConfiguration.class)
          .withPropertyValues(
              "spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
              "spring.kafka.producer.key-serializer=" + StringSerializer.class.getName(),
              "spring.kafka.producer.value-serializer=" + ByteArraySerializer.class.getName(),
              "spring.kafka.producer.properties.max.request.size=32768")
          .run(
              context -> {
                assertNull(context.getStartupFailure());
                @SuppressWarnings("unchecked")
                var original = (KafkaTemplate<String, byte[]>) context.getBean(KafkaTemplate.class);
                try (var transport = new ConsumerConfiguration().deadLetterTransport(original)) {
                  for (var template : List.of(original, transport.template())) {
                    assertThrows(
                        org.springframework.kafka.KafkaException.class,
                        () -> template.send(new ProducerRecord<>(topic, 0, privateKey, oversized)));
                  }
                }
              });
      assertEquals(
          2,
          captured.list.stream()
              .filter(
                  event -> event.getLoggerName().equals(SanitizedProducerListener.class.getName()))
              .count());
      for (var event : captured.list) {
        assertFalse(event.getFormattedMessage().contains(privateKey), event.getLoggerName());
        assertFalse(event.getFormattedMessage().contains(privatePayload), event.getLoggerName());
        if (event.getLoggerName().equals(SanitizedProducerListener.class.getName()))
          assertNull(
              event.getThrowableProxy(),
              "Producer diagnostics must not attach upstream exception text");
      }
    } finally {
      logger.detachAppender(captured);
      captured.stop();
    }
  }

  @Test
  void quarantinesANearBrokerLimitRecordBeforeAdvancingToItsValidSuffix() throws Exception {
    String topic = "dlt-bounds-" + UUID.randomUUID();
    String deadLetters = topic + ".dlt";
    String group = topic + "-consumer";
    var bootstrap = KAFKA.getBootstrapServers();
    try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
      var runtime = new RuntimeConfiguration();
      admin
          .createTopics(
              List.of(
                  runtime.eventsTopic(topic, 1, 1, 604_800_000),
                  runtime.deadLetterTopic(deadLetters, 1, 1, 604_800_000)))
          .all()
          .get(20, TimeUnit.SECONDS);
      var properties =
          Map.<String, Object>of(
              "bootstrap.servers",
              bootstrap,
              "acks",
              "all",
              "enable.idempotence",
              true,
              "key.serializer",
              StringSerializer.class,
              "value.serializer",
              ByteArraySerializer.class,
              "max.request.size",
              32768);
      var factory = new DefaultKafkaProducerFactory<String, byte[]>(properties);
      var kafka = new KafkaTemplate<>(factory);
      var external = new KafkaTemplate<>(factory, Map.of("max.request.size", 2097152));
      var consumerProperties =
          Map.<String, Object>of(
              "bootstrap.servers",
              bootstrap,
              "key.deserializer",
              StringDeserializer.class,
              "value.deserializer",
              ByteArrayDeserializer.class,
              "enable.auto.commit",
              false,
              "auto.offset.reset",
              "earliest",
              "max.poll.records",
              50);
      var consumers = new DefaultKafkaConsumerFactory<String, byte[]>(consumerProperties);
      var configuration = new ConsumerConfiguration();
      var transport = configuration.deadLetterTransport(kafka);
      assertEquals(
          32768, kafka.getProducerFactory().getConfigurationProperties().get("max.request.size"));
      var listenerFactory =
          configuration.kafkaListenerContainerFactory(
              consumers, configuration.deliveryErrors(transport, deadLetters));
      var listener = listenerFactory.createContainer(topic);
      listener.getContainerProperties().setGroupId(group);
      var stored = new AtomicInteger();
      var valid =
          new Event(UUID.randomUUID(), "dlt-bounds", "session.started", 1, Instant.EPOCH, Map.of());
      byte[] poison = new byte[1_048_000];
      new java.util.Random(17).nextBytes(poison);
      try (var meters = new ClosingMeters();
          var deadConsumer = new KafkaConsumer<String, byte[]>(consumerProperties)) {
        var persistence =
            new PersistenceConsumer(
                event -> {
                  stored.incrementAndGet();
                  return EventSink.Outcome.INSERTED;
                },
                bytes -> {
                  if (bytes.length > 1) throw new AvroEventCodec.InvalidEnvelope();
                  return valid;
                },
                new PlatformTelemetry(OpenTelemetry.noop(), meters),
                meters);
        listener.setupMessageListener(
            (BatchAcknowledgingMessageListener<String, byte[]>) persistence::consume);
        deadConsumer.assign(List.of(new TopicPartition(deadLetters, 0)));
        deadConsumer.seekToBeginning(List.of(new TopicPartition(deadLetters, 0)));
        listener.start();
        external.send(new ProducerRecord<>(topic, 0, "poison", poison)).get(10, TimeUnit.SECONDS);
        external
            .send(new ProducerRecord<>(topic, 0, valid.partitionKey(), new byte[] {1}))
            .get(10, TimeUnit.SECONDS);
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> {
                  var records = deadConsumer.poll(Duration.ofMillis(200));
                  assertEquals(1, records.count());
                  var quarantined = records.iterator().next();
                  assertArrayEquals(poison, quarantined.value());
                  int headerBytes = 0;
                  for (var header : quarantined.headers())
                    headerBytes += header.key().length() + header.value().length;
                  assertTrue(
                      poison.length + headerBytes > KafkaAcceptancePolicy.SOURCE_MAX_BYTES,
                      "Regression must exceed the source ceiling after DLT metadata is added");
                });
        await().atMost(Duration.ofSeconds(10)).until(() -> stored.get() == 1);
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(
                () ->
                    assertEquals(
                        2,
                        admin
                            .listConsumerGroupOffsets(group)
                            .partitionsToOffsetAndMetadata()
                            .get(5, TimeUnit.SECONDS)
                            .get(new TopicPartition(topic, 0))
                            .offset()));
      } finally {
        listener.stop();
        transport.close();
        external.destroy();
        kafka.destroy();
        factory.destroy();
      }
    }
  }
}
