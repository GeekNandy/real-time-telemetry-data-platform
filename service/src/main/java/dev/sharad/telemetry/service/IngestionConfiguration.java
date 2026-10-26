package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.IngestionEngine;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class IngestionConfiguration {
    @Bean
    JsonMapper jsonMapper() {
        return JsonMapper.builder().findAndAddModules()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    }

    @Bean
    WebFluxConfigurer requestLimits() {
        return new WebFluxConfigurer() {
            @Override public void configureHttpMessageCodecs(ServerCodecConfigurer codecs) {
                codecs.defaultCodecs().maxInMemorySize(16 * 1024);
            }
        };
    }

    @Bean
    NewTopic eventsTopic(@Value("${telemetry.topic}") String name,
            @Value("${telemetry.replication-factor}") int replicas,
            @Value("${telemetry.min-in-sync-replicas}") int minimum) {
        return topic(name, replicas, minimum);
    }

    @Bean
    NewTopic deadLetterTopic(@Value("${telemetry.dead-letter-topic}") String name,
            @Value("${telemetry.replication-factor}") int replicas,
            @Value("${telemetry.min-in-sync-replicas}") int minimum) {
        return topic(name, replicas, minimum);
    }

    private static NewTopic topic(String name, int replicas, int minimum) {
        if (replicas < 1 || minimum < 1 || minimum > replicas) {
            throw new IllegalArgumentException("Invalid replication policy");
        }
        return TopicBuilder.name(name).partitions(3).replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minimum))
                .config(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false").build();
    }

    @Bean(destroyMethod = "close")
    IngestionEngine ingestionEngine(KafkaTemplate<String, String> kafka, JsonMapper mapper,
            MeterRegistry metrics, @Value("${telemetry.topic}") String topic,
            @Value("${telemetry.global-capacity}") int capacity,
            @Value("${telemetry.tenant-capacity}") int tenantCapacity,
            @Value("${telemetry.send-threads}") int threads) {
        var engine = new IngestionEngine(event ->
                kafka.send(topic, event.partitionKey(), mapper.writeValueAsString(event))
                        .thenApply(sent -> new dev.sharad.telemetry.core.Broker.Position(
                                sent.getRecordMetadata().topic(), sent.getRecordMetadata().partition(),
                                sent.getRecordMetadata().offset())), capacity, tenantCapacity, threads);
        // Fixed metric names only; no event or tenant labels.
        metrics.gauge("telemetry.ingestion.inflight", engine, value -> value.snapshot().admission().inFlight());
        metrics.gauge("telemetry.ingestion.active.tenants", engine, value -> value.snapshot().admission().activeTenants());
        FunctionCounter.builder("telemetry.ingestion.accepted", engine, value -> value.snapshot().accepted())
                .register(metrics);
        FunctionCounter.builder("telemetry.ingestion.failed", engine, value -> value.snapshot().failed())
                .register(metrics);
        FunctionCounter.builder("telemetry.ingestion.rejected", engine, value -> value.snapshot().rejected())
                .register(metrics);
        return engine;
    }
}
