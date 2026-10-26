package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.IngestionEngine;
import jakarta.validation.Valid;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
public class EventsController {
    private final IngestionEngine engine;
    private final PostgresEventSink sink;

    public EventsController(IngestionEngine engine, PostgresEventSink sink) {
        this.engine = engine;
        this.sink = sink;
    }

    @PostMapping("/v1/events")
    public Mono<ResponseEntity<IngestionEngine.Acceptance>> ingest(
            @AuthenticationPrincipal Jwt jwt, @Valid @RequestBody EventRequest request) {
        return Mono.defer(() -> Mono.fromCompletionStage(engine.accept(request.forTenant(jwt.getClaimAsString("tenant_id")))))
                .map(receipt -> ResponseEntity.accepted().body(receipt));
    }

    @GetMapping("/v1/events/{id}")
    public Mono<ResponseEntity<Map<String, Object>>> get(
            @AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return Mono.fromCallable(() -> sink.find(jwt.getClaimAsString("tenant_id"), id))
                .subscribeOn(Schedulers.boundedElastic())
                .map(event -> event.<ResponseEntity<Map<String, Object>>>map(ResponseEntity::ok)
                        .orElseGet(() -> ResponseEntity.notFound().build()));
    }
}
