package dev.sharad.telemetry.observability;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PlatformTelemetryTest {
  @Test
  void propagatesOneTraceAcrossAnAsynchronousCarrierWithoutLeakingBaggage() {
    var exporter = InMemorySpanExporter.create();
    var registry = new SimpleMeterRegistry();
    try (var provider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build()) {
      var telemetry =
          new PlatformTelemetry(
              OpenTelemetrySdk.builder().setTracerProvider(provider).build(), registry);
      var ingress = telemetry.start(PlatformTelemetry.Stage.INGRESS, SpanKind.SERVER, Map.of());
      Map<String, String> carrier;
      try (var ignored =
          Baggage.builder().put("private-data", "must-not-propagate").build().makeCurrent()) {
        carrier = telemetry.inject(ingress);
      }
      ingress.end();
      var persistence =
          telemetry.start(PlatformTelemetry.Stage.PERSISTENCE, SpanKind.CONSUMER, carrier);
      persistence.end();
      var spans = exporter.getFinishedSpanItems();
      assertEquals(2, spans.size());
      assertEquals(spans.get(0).getTraceId(), spans.get(1).getTraceId());
      assertEquals(spans.get(0).getSpanId(), spans.get(1).getParentSpanId());
      assertTrue(carrier.containsKey("traceparent"));
      assertFalse(carrier.containsKey("baggage"));
      assertEquals(1, carrier.size());
    } finally {
      registry.close();
    }
  }
}
