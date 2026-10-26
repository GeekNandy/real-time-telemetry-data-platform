package dev.sharad.telemetry.service;

import java.util.Map;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@RestController
@Profile("!collector")
public class EventQueryController {
  private final PostgresEventSink sink;
  private final Scheduler jdbcScheduler;

  public EventQueryController(PostgresEventSink sink, Scheduler jdbcScheduler) {
    this.sink = sink;
    this.jdbcScheduler = jdbcScheduler;
  }

  @GetMapping("/v1/events/{id}")
  public Mono<ResponseEntity<Map<String, Object>>> get(
      @AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
    return Mono.fromCallable(() -> sink.find(jwt.getClaimAsString("tenant_id"), id))
        .subscribeOn(jdbcScheduler)
        .map(
            event ->
                event
                    .<ResponseEntity<Map<String, Object>>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build()));
  }
}
