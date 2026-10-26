package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.test.web.reactive.server.WebTestClient;

@WebFluxTest(
    controllers = OperationsController.class,
    properties = {
      "telemetry.component=collector",
      "telemetry.security.mode=demo",
      "telemetry.security.issuer=https://issuer.test",
      "telemetry.security.demo-secret=test-only-key-with-at-least-thirty-two-bytes"
    })
@Import({
  SecurityConfiguration.class,
  OperationsConfiguration.class,
  OperationsControllerTest.Meters.class
})
class OperationsControllerTest {
  @Autowired WebTestClient client;
  @Autowired MeterRegistry meters;

  @TestConfiguration
  @EnableWebFluxSecurity
  static class Meters {
    @Bean
    MeterRegistry meters() {
      return new SimpleMeterRegistry();
    }
  }

  @BeforeEach
  void clearMeters() {
    meters.clear();
  }

  @Test
  void requiresAnAuthenticatedObserver() {
    client.get().uri("/v1/operations/metrics").exchange().expectStatus().isUnauthorized();
  }

  @Test
  void eventReadAndWriteScopesDoNotGrantGlobalMetrics() throws Exception {
    String token = TestTokens.tenant("tenant-a");
    client
        .get()
        .uri("/v1/operations/metrics")
        .headers(headers -> headers.setBearerAuth(token))
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  @Test
  void exposesOnlyFixedAggregateMetricsToAnObserver() throws Exception {
    var count = new AtomicLong(8);
    FunctionCounter.builder("telemetry.ingestion.accepted", count, AtomicLong::doubleValue)
        .register(meters);
    Gauge.builder("telemetry.buffer.records", () -> 3.0).strongReference(true).register(meters);
    Gauge.builder("telemetry.buffer.bytes", () -> 1024.0).strongReference(true).register(meters);
    Gauge.builder("telemetry.buffer.oldest.age", () -> 2.5).strongReference(true).register(meters);
    meters.counter("tenant-secret", "event-id", "private-event").increment();
    observe()
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .valueEquals("Cache-Control", "no-store")
        .expectBody()
        .jsonPath("$.component")
        .isEqualTo("collector")
        .jsonPath("$.acceptedTotal")
        .isEqualTo(8.0)
        .jsonPath("$.bufferRecords")
        .isEqualTo(3.0)
        .jsonPath("$.bufferBytes")
        .isEqualTo(1024.0)
        .jsonPath("$.oldestBufferedAgeSeconds")
        .isEqualTo(2.5)
        .jsonPath("$.instanceId")
        .isNotEmpty()
        .consumeWith(
            result -> {
              String json =
                  new String(result.getResponseBody(), java.nio.charset.StandardCharsets.UTF_8);
              assertFalse(json.contains("tenant-secret"));
              assertFalse(json.contains("private-event"));
            });
    java.lang.ref.Reference.reachabilityFence(count);
  }

  @Test
  void absentMetricsAreUnknownRatherThanInventedZeros() throws Exception {
    observe()
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.acceptedTotal")
        .isEmpty()
        .jsonPath("$.bufferRecords")
        .isEmpty()
        .jsonPath("$.stages.persistence.completed")
        .isEmpty();
  }

  @Test
  void nonfiniteGaugeValuesRemainUnknown() throws Exception {
    Gauge.builder("telemetry.buffer.records", () -> Double.NaN)
        .strongReference(true)
        .register(meters);
    observe().exchange().expectStatus().isOk().expectBody().jsonPath("$.bufferRecords").isEmpty();
  }

  @Test
  void evaluatesMeterCallbacksOnTheDedicatedWorker() throws Exception {
    var thread = new AtomicReference<String>();
    Gauge.builder(
            "telemetry.buffer.records",
            () -> {
              thread.set(Thread.currentThread().getName());
              assertFalse(reactor.core.scheduler.Schedulers.isInNonBlockingThread());
              return 3.0;
            })
        .strongReference(true)
        .register(meters);
    observe().exchange().expectStatus().isOk();
    assertTrue(thread.get().startsWith("telemetry-operations-"));
  }

  @Test
  void aContendedBufferGaugeTimesOutWhileTheRequestThreadRemainsResponsive() throws Exception {
    Object gate = new Object();
    var locked = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var entered = new CountDownLatch(1);
    Thread owner =
        Thread.ofPlatform()
            .start(
                () -> {
                  synchronized (gate) {
                    locked.countDown();
                    try {
                      release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException error) {
                      Thread.currentThread().interrupt();
                    }
                  }
                });
    try {
      assertTrue(locked.await(2, TimeUnit.SECONDS));
      Gauge.builder(
              "telemetry.buffer.records",
              () -> {
                entered.countDown();
                synchronized (gate) {
                  return 3.0;
                }
              })
          .strongReference(true)
          .register(meters);
      observe()
          .exchange()
          .expectStatus()
          .isEqualTo(503)
          .expectHeader()
          .valueEquals("Cache-Control", "no-store");
      assertEquals(0, entered.getCount());
      assertEquals(1, release.getCount());
    } finally {
      release.countDown();
      owner.join(5000);
    }
  }

  private WebTestClient.RequestHeadersSpec<?> observe() throws Exception {
    String token =
        TestTokens.sign(
            TestTokens.claims("tenant-a").claim("scope", "telemetry.observe").build(),
            TestTokens.SECRET);
    return client
        .get()
        .uri("/v1/operations/metrics")
        .headers(headers -> headers.setBearerAuth(token));
  }
}
