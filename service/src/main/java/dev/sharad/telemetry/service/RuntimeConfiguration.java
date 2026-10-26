package dev.sharad.telemetry.service;

import dev.sharad.telemetry.config.ProductionPolicy;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import dev.sharad.telemetry.schema.AvroEventCodec;
import dev.sharad.telemetry.schema.RegistryClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.net.URI;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class RuntimeConfiguration {
  @Bean
  static org.springframework.beans.factory.config.BeanFactoryPostProcessor productionPolicy(
      org.springframework.core.env.Environment environment) {
    return factory ->
        ProductionPolicy.validate(
            "production".equals(environment.getProperty("telemetry.deployment", "development")),
            environment.getProperty("telemetry.security.mode", "issuer"),
            environment.getProperty("telemetry.security.issuer", ""),
            environment.getProperty("telemetry.schema.url", ""),
            environment.getProperty("spring.kafka.properties.security.protocol", "PLAINTEXT"),
            environment.getProperty("spring.datasource.url", ""),
            !environment.acceptsProfiles(org.springframework.core.env.Profiles.of("collector")));
  }

  @Bean
  JsonMapper jsonMapper() {
    return JsonMapper.builder()
        .findAndAddModules()
        .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();
  }

  @Bean
  WebFluxConfigurer requestLimits() {
    return new WebFluxConfigurer() {
      @Override
      public void configureHttpMessageCodecs(ServerCodecConfigurer codecs) {
        codecs.defaultCodecs().maxInMemorySize(16 * 1024);
      }
    };
  }

  @Bean
  RegistryClient schemaRegistry(
      @Value("${telemetry.schema.url}") URI url,
      @Value("${telemetry.schema.allow-registration:false}") boolean registration,
      @Value("${telemetry.schema.authorization:}") String authorization) {
    var client = new RegistryClient(url, registration, authorization);
    if (registration) client.compatibility(AvroEventCodec.SUBJECT, "FULL_TRANSITIVE");
    return client;
  }

  @Bean
  AvroEventCodec eventCodec(RegistryClient client) {
    return new AvroEventCodec(client);
  }

  @Bean(destroyMethod = "close")
  SdkTracerProvider tracerProvider(
      @Value("${telemetry.tracing.endpoint:}") String endpoint,
      @Value("${telemetry.tracing.sample-ratio:1.0}") double ratio,
      @Value("${spring.application.name}") String name,
      @Value("${telemetry.component:combined}") String component) {
    var builder =
        SdkTracerProvider.builder()
            .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(ratio)))
            .setResource(
                Resource.getDefault()
                    .merge(
                        Resource.create(
                            Attributes.of(
                                AttributeKey.stringKey("service.name"), name + "-" + component))));
    if (!endpoint.isBlank())
      builder.addSpanProcessor(
          BatchSpanProcessor.builder(
                  OtlpGrpcSpanExporter.builder()
                      .setEndpoint(endpoint)
                      .setTimeout(Duration.ofSeconds(3))
                      .build())
              .build());
    return builder.build();
  }

  @Bean
  PlatformTelemetry platformTelemetry(SdkTracerProvider provider, MeterRegistry meters) {
    return new PlatformTelemetry(
        OpenTelemetrySdk.builder().setTracerProvider(provider).build(), meters);
  }

  @Bean
  NewTopic eventsTopic(
      @Value("${telemetry.topic}") String name,
      @Value("${telemetry.replication-factor}") int replicas,
      @Value("${telemetry.min-in-sync-replicas}") int minimum,
      @Value("${telemetry.retention.log-ms}") long retention) {
    return topic(name, replicas, minimum, retention);
  }

  @Bean
  NewTopic deadLetterTopic(
      @Value("${telemetry.dead-letter-topic}") String name,
      @Value("${telemetry.replication-factor}") int replicas,
      @Value("${telemetry.min-in-sync-replicas}") int minimum,
      @Value("${telemetry.retention.log-ms}") long retention) {
    return topic(name, replicas, minimum, retention);
  }

  private NewTopic topic(String name, int replicas, int minimum, long retention) {
    if (replicas < 1 || minimum < 1 || minimum > replicas || retention < 60000)
      throw new IllegalArgumentException("Invalid broker policy");
    return TopicBuilder.name(name)
        .partitions(3)
        .replicas(replicas)
        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minimum))
        .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retention))
        .config(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
        .build();
  }
}
