package dev.sharad.telemetry.mcp;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OperatorBoundsTest {
  @Test
  void cancellationInterruptsTheReadAndReleasesItsWorker() throws Exception {
    try (var tools = new OperatorTools(OperatorConfig.from(Map.of()))) {
      var entered = new CountDownLatch(1);
      var interrupted = new CountDownLatch(1);
      var subscription =
          tools
              .execute(
                  "cancel_test",
                  () -> {
                    entered.countDown();
                    try {
                      new CountDownLatch(1).await();
                    } catch (InterruptedException error) {
                      interrupted.countDown();
                      Thread.currentThread().interrupt();
                    }
                    return OperatorTools.error("CANCELLED");
                  })
              .subscribe();
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      subscription.dispose();
      assertTrue(interrupted.await(2, TimeUnit.SECONDS));
      assertNotNull(
          tools
              .execute("cancel_test", () -> OperatorTools.error("RECOVERED"))
              .block(Duration.ofSeconds(2)));
    }
  }

  @Test
  void rejectsCredentialsAndArbitraryUnencryptedDestinations() {
    for (String uri :
        new String[] {
          "http://example.com", "http://127.0.0.1/private", "https://user:secret@example.com",
          "https://example.com?token=secret", "https://example.com#secret", "file:///etc/passwd"
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new OperatorConfig(URI.create(uri), URI.create("http://127.0.0.1:8082"), null));
    }
  }

  @Test
  void permitsOnlyStartupConfiguredOriginsAndAbsoluteCredentialPath() {
    assertEquals(
        "https",
        OperatorConfig.from(Map.of("TELEMETRY_COLLECTOR_URL", "https://collector.example.test"))
            .collector()
            .getScheme());
    assertDoesNotThrow(
        () -> OperatorConfig.from(Map.of("TELEMETRY_COLLECTOR_URL", "http://[::1]:8080/")));
    assertThrows(
        IllegalArgumentException.class,
        () -> OperatorConfig.from(Map.of("TELEMETRY_OBSERVE_TOKEN_FILE", "relative.token")));
  }

  @Test
  void saturatingTheReadSchedulerRejectsWorkAndRecovers() throws Exception {
    var config = OperatorConfig.from(Map.of());
    try (var tools = new OperatorTools(config)) {
      var entered = new CountDownLatch(2);
      var release = new CountDownLatch(1);
      var running = new AtomicInteger();
      var maximum = new AtomicInteger();
      var pending =
          new ArrayList<
              java.util.concurrent.CompletableFuture<
                  io.modelcontextprotocol.spec.McpSchema.CallToolResult>>();
      try {
        for (int index = 0; index < 40; index++) {
          pending.add(
              tools
                  .execute(
                      "bounded_test",
                      () -> {
                        int active = running.incrementAndGet();
                        maximum.accumulateAndGet(active, Math::max);
                        entered.countDown();
                        try {
                          release.await(4, TimeUnit.SECONDS);
                        } catch (InterruptedException error) {
                          Thread.currentThread().interrupt();
                        } finally {
                          running.decrementAndGet();
                        }
                        return OperatorTools.error("TEST_COMPLETE");
                      })
                  .toFuture());
        }
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        assertEquals(2, maximum.get());
        assertTrue(
            pending.stream().filter(java.util.concurrent.CompletableFuture::isDone).count() >= 30);
      } finally {
        release.countDown();
      }
      for (var future : pending) {
        assertNotNull(future.get(3, TimeUnit.SECONDS));
      }
      assertEquals(
          "{error=RECOVERED}",
          tools
              .execute("bounded_test", () -> OperatorTools.error("RECOVERED"))
              .block(Duration.ofSeconds(2))
              .structuredContent()
              .toString());
    }
  }
}
