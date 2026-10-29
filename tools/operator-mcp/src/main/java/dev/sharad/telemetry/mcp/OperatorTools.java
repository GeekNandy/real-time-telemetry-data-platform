package dev.sharad.telemetry.mcp;

import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

final class OperatorTools implements AutoCloseable {
  private static final McpJsonMapper JSON = McpJsonDefaults.getMapper();
  private final OperatorConfig config;
  private final BoundedHttp http;
  // At most two calls run, with four waiting tasks per worker. No ingestion scheduler is shared.
  private final Scheduler calls = Schedulers.newBoundedElastic(2, 4, "operator-read");

  OperatorTools(OperatorConfig config) {
    this(config, new BoundedHttp());
  }

  OperatorTools(OperatorConfig config, BoundedHttp http) {
    this.config = config;
    this.http = http;
  }

  List<AsyncToolSpecification> specifications() {
    var empty = schema(Map.of(), List.of());
    return List.of(
        tool(
            "get_platform_health",
            "Read public collector and worker health status.",
            empty,
            true,
            args ->
                Map.of("collector", health(config.collector()), "worker", health(config.worker()))),
        tool(
            "get_ingestion_metrics",
            "Read aggregate metrics; requires telemetry.observe permission.",
            schema(
                Map.of(
                    "component", Map.of("type", "string", "enum", List.of("collector", "worker"))),
                List.of("component")),
            true,
            args -> metrics((String) args.get("component"))),
        tool(
            "get_event_schema",
            "Read the bundled Avro schema for version 1 or 2.",
            schema(
                Map.of("version", Map.of("type", "integer", "enum", List.of(1, 2))),
                List.of("version")),
            false,
            args ->
                resource(
                    "/avro/telemetry-v" + ((Number) args.get("version")).intValue() + ".avsc")),
        tool(
            "get_benchmark_results",
            "Read bundled synthetic evidence and fixed SLO profiles; not live measurements.",
            empty,
            false,
            args ->
                Map.of(
                    "evidence",
                    resource("/evidence/operator-summary.json"),
                    "profiles",
                    resource("/evidence/profiles.json"))));
  }

  private AsyncToolSpecification tool(
      String name, String description, Map<String, Object> schema, boolean network, Read read) {
    var definition =
        McpSchema.Tool.builder(name, schema)
            .description(description)
            .annotations(
                McpSchema.ToolAnnotations.builder()
                    .readOnlyHint(true)
                    .destructiveHint(false)
                    .idempotentHint(true)
                    .openWorldHint(network)
                    .build())
            .build();
    return AsyncToolSpecification.builder()
        .tool(definition)
        .callHandler(
            (exchange, request) ->
                execute(
                    name,
                    () -> {
                      try {
                        return result(read.get(request.arguments()), false);
                      } catch (BoundedHttp.Failure error) {
                        return error(error.getMessage());
                      } catch (Exception error) {
                        return error("READ_UNAVAILABLE");
                      }
                    }))
        .build();
  }

  Mono<McpSchema.CallToolResult> execute(String name, Supplier<McpSchema.CallToolResult> read) {
    return Mono.defer(
        () -> {
          long began = System.nanoTime();
          return Mono.fromSupplier(read)
              .subscribeOn(calls)
              .timeout(Duration.ofSeconds(7))
              .onErrorReturn(error("READ_BUSY_OR_TIMED_OUT"))
              .doOnNext(
                  value ->
                      System.err.printf(
                          "operator tool=%s outcome=%s elapsed_ms=%d%n",
                          name,
                          Boolean.TRUE.equals(value.isError()) ? "unavailable" : "ok",
                          (System.nanoTime() - began) / 1_000_000));
        });
  }

  private Map<String, Object> health(URI origin) {
    try {
      Map<?, ?> body = object(http.get(origin, "/actuator/health", null));
      Object status = body.get("status");
      if (!Set.of("UP", "DOWN", "OUT_OF_SERVICE", "UNKNOWN").contains(status)) {
        return Map.of("status", "UNKNOWN");
      }
      return Map.of("status", status);
    } catch (Exception error) {
      return Map.of("status", "UNAVAILABLE");
    }
  }

  private Map<String, Object> metrics(String component) throws Exception {
    URI origin =
        switch (component) {
          case "collector" -> config.collector();
          case "worker" -> config.worker();
          default -> throw new BoundedHttp.Failure("INVALID_COMPONENT");
        };
    var body = object(http.get(origin, "/v1/operations/metrics", token()));
    var filtered = new LinkedHashMap<String, Object>();
    filtered.put("component", component);
    if (body.get("observedAt") instanceof String date) {
      filtered.put("observedAt", Instant.parse(date).toString());
    }
    for (String field :
        List.of(
            "acceptedTotal",
            "bufferRecords",
            "bufferBytes",
            "oldestBufferedAgeSeconds",
            "appendPendingRecords",
            "appendPendingBytes")) {
      filtered.put(field, finite(body.get(field)));
    }
    var stages = new LinkedHashMap<String, Object>();
    if (body.get("stages") instanceof Map<?, ?> values) {
      for (String name : List.of("ingress", "dispatch", "persistence")) {
        if (values.get(name) instanceof Map<?, ?> stage) {
          var counters = new LinkedHashMap<String, Object>();
          counters.put("completed", finite(stage.get("completed")));
          counters.put("failures", finite(stage.get("failures")));
          stages.put(name, counters);
        }
      }
    }
    filtered.put("stages", stages);
    return filtered;
  }

  private String token() throws IOException, BoundedHttp.Failure {
    if (config.tokenFile() == null) {
      throw new BoundedHttp.Failure("OBSERVE_TOKEN_NOT_CONFIGURED");
    }
    if (Files.getFileStore(config.tokenFile()).supportsFileAttributeView("posix")) {
      var permissions = Files.getPosixFilePermissions(config.tokenFile());
      if (permissions.stream()
          .anyMatch(
              permission ->
                  permission != PosixFilePermission.OWNER_READ
                      && permission != PosixFilePermission.OWNER_WRITE)) {
        throw new BoundedHttp.Failure("OBSERVE_TOKEN_FILE_NOT_PRIVATE");
      }
    }
    byte[] bytes;
    try (var stream = Files.newInputStream(config.tokenFile())) {
      bytes = stream.readNBytes(8195);
    }
    String token = new String(bytes, StandardCharsets.US_ASCII).stripTrailing();
    if (bytes.length > 8194 || !token.matches("[A-Za-z0-9._-]{1,8192}")) {
      throw new BoundedHttp.Failure("OBSERVE_TOKEN_INVALID");
    }
    return token;
  }

  private static Object finite(Object value) {
    return value instanceof Number number
            && Double.isFinite(number.doubleValue())
            && number.doubleValue() >= 0
        ? number
        : null;
  }

  private static Map<?, ?> object(byte[] bytes) throws IOException {
    if (JSON.readValue(bytes, Object.class) instanceof Map<?, ?> map) {
      return map;
    }
    throw new IOException("INVALID_UPSTREAM_RESPONSE");
  }

  private static Object resource(String path) throws IOException {
    try (var stream = OperatorTools.class.getResourceAsStream(path)) {
      if (stream == null) {
        throw new IOException("MISSING_BUNDLED_EVIDENCE");
      }
      return JSON.readValue(stream.readNBytes(64 * 1024), Object.class);
    }
  }

  private static Map<String, Object> schema(Map<String, Object> properties, List<String> required) {
    return Map.of(
        "type",
        "object",
        "properties",
        properties,
        "required",
        required,
        "additionalProperties",
        false);
  }

  static McpSchema.CallToolResult error(String code) {
    return result(Map.of("error", code), true);
  }

  private static McpSchema.CallToolResult result(Object data, boolean error) {
    try {
      return McpSchema.CallToolResult.builder()
          .addTextContent(JSON.writeValueAsString(data))
          .structuredContent(data)
          .isError(error)
          .build();
    } catch (IOException failure) {
      throw new IllegalStateException("SERIALIZATION_UNAVAILABLE");
    }
  }

  @Override
  public void close() {
    calls.dispose();
    http.close();
  }

  @FunctionalInterface
  private interface Read {
    Object get(Map<String, Object> arguments) throws Exception;
  }
}
