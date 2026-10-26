package dev.sharad.telemetry.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared metrics and W3C tracing conventions. Event IDs, tenant IDs and payloads are never metric
 * labels.
 */
public final class PlatformTelemetry {
  public enum Stage {
    INGRESS,
    DISPATCH,
    PERSISTENCE
  }

  private final Tracer tracer;
  private final Map<Stage, Timer> durations = new EnumMap<>(Stage.class);
  private final Map<Stage, Counter> failures = new EnumMap<>(Stage.class);
  private static final TextMapGetter<Map<String, String>> GETTER =
      new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
          return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
          return carrier.get(key);
        }
      };

  public PlatformTelemetry(OpenTelemetry telemetry, MeterRegistry meters) {
    tracer = telemetry.getTracer("dev.sharad.telemetry.platform", "0.1.0");
    for (Stage stage : Stage.values()) {
      String label = stage.name().toLowerCase(java.util.Locale.ROOT);
      durations.put(
          stage,
          Timer.builder("telemetry.stage.duration")
              .tag("stage", label)
              .minimumExpectedValue(Duration.ofMillis(1))
              .maximumExpectedValue(Duration.ofSeconds(30))
              .publishPercentileHistogram()
              .register(meters));
      failures.put(
          stage, Counter.builder("telemetry.stage.failures").tag("stage", label).register(meters));
    }
  }

  public Span start(Stage stage, SpanKind kind, Map<String, String> headers) {
    Context parent =
        W3CTraceContextPropagator.getInstance().extract(Context.root(), headers, GETTER);
    return tracer
        .spanBuilder("telemetry." + stage.name().toLowerCase(java.util.Locale.ROOT))
        .setParent(parent)
        .setSpanKind(kind)
        .startSpan();
  }

  public Map<String, String> inject(Span span) {
    var result = new HashMap<String, String>();
    W3CTraceContextPropagator.getInstance().inject(Context.root().with(span), result, Map::put);
    return Map.copyOf(result); // Baggage and Authorization are deliberately excluded.
  }

  public void completed(Stage stage, long startedNanos, boolean successful) {
    durations
        .get(stage)
        .record(
            Math.max(0, System.nanoTime() - startedNanos),
            java.util.concurrent.TimeUnit.NANOSECONDS);
    if (!successful) failures.get(stage).increment();
  }
}
