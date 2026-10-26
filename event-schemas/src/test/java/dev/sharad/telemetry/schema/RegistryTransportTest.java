package dev.sharad.telemetry.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RegistryTransportTest {
  private HttpServer server;
  private ExecutorService handlers;
  private final CountDownLatch release = new CountDownLatch(1);

  @AfterEach
  void stop() throws Exception {
    release.countDown();
    if (server != null) server.stop(0);
    if (handlers != null) {
      handlers.shutdownNow();
      assertTrue(handlers.awaitTermination(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void responseDeadlineIncludesAStalledBodyAfterHeaders() throws Exception {
    var bodyStarted = new AtomicBoolean();
    RegistryClient client =
        serve(
            exchange -> {
              exchange.sendResponseHeaders(200, 0);
              exchange.getResponseBody().write('{');
              exchange.getResponseBody().flush();
              bodyStarted.set(true);
              try {
                release.await();
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              } finally {
                exchange.close();
              }
            });
    var failure =
        assertTimeoutPreemptively(
            Duration.ofSeconds(5),
            () ->
                assertThrows(
                    SchemaCatalog.Unavailable.class,
                    () -> client.registerOrLookup("events", EventSchemas.v1())));
    assertTrue(bodyStarted.get(), "The response body must have started before stalling");
    assertInstanceOf(IOException.class, failure.getCause());
  }

  @Test
  void acceptsACompleteResponseExactlyAtTheByteBudget() throws Exception {
    byte[] response = new byte[65536];
    Arrays.fill(response, (byte) ' ');
    byte[] json = "{\"id\":7}".getBytes(StandardCharsets.UTF_8);
    System.arraycopy(json, 0, response, 0, json.length);
    RegistryClient client =
        serve(
            exchange -> {
              exchange.sendResponseHeaders(200, response.length);
              try (var output = exchange.getResponseBody()) {
                output.write(response);
              }
            });
    assertEquals(7, client.registerOrLookup("events", EventSchemas.v1()));
  }

  @Test
  void rejectsAnOversizedChunkedResponse() throws Exception {
    RegistryClient client =
        serve(
            exchange -> {
              exchange.sendResponseHeaders(200, 0);
              try (var output = exchange.getResponseBody()) {
                output.write(new byte[70000]);
              } catch (IOException cancelled) {
                // The client is expected to cancel once its response budget is exceeded.
              }
            });
    assertUnavailable(client);
  }

  @Test
  void rejectsAnOversizedFixedLengthResponse() throws Exception {
    RegistryClient client =
        serve(
            exchange -> {
              exchange.sendResponseHeaders(200, 70000);
              try (var output = exchange.getResponseBody()) {
                output.write(new byte[70000]);
              } catch (IOException cancelled) {
                // Cancellation is the successful bounded-reader outcome.
              }
            });
    assertUnavailable(client);
  }

  @Test
  void keepsRegistryFailuresRetryable() throws Exception {
    RegistryClient client =
        serve(
            exchange -> {
              byte[] response = "{\"error_code\":503}".getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(503, response.length);
              try (var output = exchange.getResponseBody()) {
                output.write(response);
              }
            });
    assertUnavailable(client);
  }

  private RegistryClient serve(HttpHandler handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    handlers = Executors.newCachedThreadPool();
    server.setExecutor(handlers);
    server.createContext("/", handler);
    server.start();
    return new RegistryClient(
        URI.create("http://127.0.0.1:" + server.getAddress().getPort()), false, "");
  }

  private static void assertUnavailable(RegistryClient client) {
    var failure =
        assertThrows(
            SchemaCatalog.Unavailable.class,
            () -> client.registerOrLookup("events", EventSchemas.v1()));
    assertInstanceOf(IOException.class, failure.getCause());
  }
}
