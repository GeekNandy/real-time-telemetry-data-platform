package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.IngestionEngine;
import dev.sharad.telemetry.core.Submission;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import jakarta.validation.Valid;
import java.util.HashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@Profile("!worker")
public class EventsController {
  private final IngestionEngine engine;
  private final PlatformTelemetry telemetry;

  public EventsController(IngestionEngine engine, PlatformTelemetry telemetry) {
    this.engine = engine;
    this.telemetry = telemetry;
  }

  @PostMapping("/v1/events")
  public Mono<ResponseEntity<IngestionEngine.Acceptance>> ingest(
      @AuthenticationPrincipal Jwt jwt,
      @Valid @RequestBody EventRequest request,
      @RequestHeader HttpHeaders headers) {
    return Mono.defer(
        () -> {
          var parent = new HashMap<String, String>();
          for (String key : new String[] {"traceparent", "tracestate"}) {
            String value = headers.getFirst(key);
            if (value != null && value.length() <= 512) parent.put(key, value);
          }
          var span = telemetry.start(PlatformTelemetry.Stage.INGRESS, SpanKind.SERVER, parent);
          long started = System.nanoTime();
          try {
            var pending =
                engine.accept(
                    new Submission(
                        request.forTenant(jwt.getClaimAsString("tenant_id")),
                        telemetry.inject(span)));
            pending.whenComplete(
                (receipt, error) -> {
                  if (error != null) span.setStatus(StatusCode.ERROR, "Durable append unconfirmed");
                  telemetry.completed(PlatformTelemetry.Stage.INGRESS, started, error == null);
                  span.end();
                });
            return Mono.fromCompletionStage(pending)
                .map(receipt -> ResponseEntity.accepted().body(receipt));
          } catch (RuntimeException failure) {
            span.setStatus(StatusCode.ERROR, "Intake rejected");
            span.end();
            telemetry.completed(PlatformTelemetry.Stage.INGRESS, started, false);
            return Mono.error(failure);
          }
        });
  }
}
