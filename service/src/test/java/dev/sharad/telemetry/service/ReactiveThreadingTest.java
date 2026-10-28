package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

class ReactiveThreadingTest {
  @Test
  void blockingDetectorCanaryMustRejectBlockingOnReactiveThreads() {
    StepVerifier.create(
            Mono.fromCallable(
                    () -> {
                      Thread.sleep(1);
                      return 1;
                    })
                .subscribeOn(Schedulers.parallel()))
        .expectError(BlockingOperationError.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void blockedJdbcQueriesDoNotBlockTheNettyEventLoopAndCancellationReleasesWork() throws Exception {
    var entered = new CountDownLatch(4);
    var release = new CountDownLatch(1);
    var exited = new CountDownLatch(4);
    var cancelled = new CountDownLatch(1);
    var active = new AtomicInteger();
    var forbiddenThread = new java.util.concurrent.atomic.AtomicBoolean();
    var jdbc =
        new JdbcTemplate() {
          @Override
          @SuppressWarnings("unchecked")
          public <T> List<T> query(String sql, RowMapper<T> mapper, Object... arguments) {
            if (Schedulers.isInNonBlockingThread()) forbiddenThread.set(true);
            active.incrementAndGet();
            entered.countDown();
            try {
              if (!release.await(10, TimeUnit.SECONDS))
                throw new IllegalStateException("Fixture release deadline");
              return (List<T>) List.of(Map.of("eventId", arguments[1].toString()));
            } catch (InterruptedException interrupted) {
              cancelled.countDown();
              Thread.currentThread().interrupt();
              return List.of();
            } finally {
              active.decrementAndGet();
              exited.countDown();
            }
          }
        };
    var scheduler = new DatabaseConfiguration().jdbcScheduler();
    var controller =
        new EventQueryController(
            new PostgresEventSink(
                jdbc,
                new org.springframework.transaction.support.TransactionTemplate(),
                JsonMapper.builder().build()),
            scheduler);
    var jwt =
        Jwt.withTokenValue("synthetic-fixture")
            .header("alg", "fixture")
            .claim("tenant_id", "reactive-fixture")
            .build();
    var eventId = UUID.randomUUID();
    var router =
        RouterFunctions.route()
            .GET(
                "/event",
                request ->
                    controller
                        .get(jwt, eventId)
                        .flatMap(
                            response ->
                                ServerResponse.status(response.getStatusCode())
                                    .bodyValue(response.getBody())))
            .GET("/health", request -> ServerResponse.ok().bodyValue("ready"))
            .build();
    var server =
        HttpServer.create()
            .host("127.0.0.1")
            .port(0)
            .handle(new ReactorHttpHandlerAdapter(RouterFunctions.toHttpHandler(router)))
            .bindNow();
    var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    try {
      var queries = new ArrayList<java.util.concurrent.CompletableFuture<HttpResponse<String>>>();
      for (int i = 0; i < 4; i++)
        queries.add(
            http.sendAsync(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/event"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString()));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertTrue(queries.stream().noneMatch(java.util.concurrent.CompletableFuture::isDone));
      var health =
          http.send(
              HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/health"))
                  .timeout(Duration.ofSeconds(2))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(200, health.statusCode());
      assertEquals("ready", health.body());
      assertFalse(forbiddenThread.get());
      assertTrue(queries.getFirst().cancel(true));
      assertTrue(
          cancelled.await(5, TimeUnit.SECONDS),
          "Cancelled HTTP query must interrupt bounded JDBC work");
      release.countDown();
      for (int i = 1; i < queries.size(); i++)
        assertEquals(200, queries.get(i).get(5, TimeUnit.SECONDS).statusCode());
      assertTrue(exited.await(5, TimeUnit.SECONDS));
      assertEquals(0, active.get());
    } finally {
      release.countDown();
      server.disposeNow();
      http.close();
      scheduler.dispose();
    }
  }
}
