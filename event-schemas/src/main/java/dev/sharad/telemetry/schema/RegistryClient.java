package dev.sharad.telemetry.schema;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import tools.jackson.databind.json.JsonMapper;

/** Confluent-compatible REST client. Writers preload bundled schemas; registry cache is bounded. */
public final class RegistryClient implements SchemaCatalog {
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);
  private static final int MAX_RESPONSE_BYTES = 65536;
  private final URI base;
  private final boolean registrationAllowed;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private final JsonMapper json = JsonMapper.builder().build();
  private final Map<Integer, Schema> cache = new LinkedHashMap<>();
  private final String authorization;

  public RegistryClient(URI base, boolean registrationAllowed, String authorization) {
    if (!"http".equals(base.getScheme()) && !"https".equals(base.getScheme()))
      throw new IllegalArgumentException("Registry URL");
    this.base = base;
    this.registrationAllowed = registrationAllowed;
    this.authorization = authorization;
  }

  @Override
  public int registerOrLookup(String subject, Schema schema) {
    String path = "/subjects/" + URLEncoder.encode(subject, StandardCharsets.UTF_8);
    String body =
        json.writeValueAsString(Map.of("schemaType", "AVRO", "schema", schema.toString()));
    var lookup = request("POST", path, body);
    if (lookup.statusCode() == 404 && registrationAllowed)
      lookup = request("POST", path + "/versions?normalize=true", body);
    if (lookup.statusCode() == 409 || lookup.statusCode() == 422) throw new Incompatible();
    requireSuccess(lookup.statusCode());
    int id = json.readTree(lookup.body()).get("id").asInt();
    if (id < 1) throw new Incompatible();
    remember(id, schema);
    return id;
  }

  @Override
  public Schema byId(int id) {
    synchronized (cache) {
      Schema hit = cache.get(id);
      if (hit != null) return hit;
    }
    var result = request("GET", "/schemas/ids/" + id, null);
    if (result.statusCode() == 404) throw new AvroEventCodec.InvalidEnvelope();
    requireSuccess(result.statusCode());
    Schema schema =
        new Schema.Parser().parse(json.readTree(result.body()).get("schema").asString());
    remember(id, schema);
    return schema;
  }

  public void compatibility(String subject, String mode) {
    if (!registrationAllowed)
      throw new IllegalStateException("Registry configuration is read-only");
    var response =
        request(
            "PUT",
            "/config/" + URLEncoder.encode(subject, StandardCharsets.UTF_8),
            json.writeValueAsString(Map.of("compatibility", mode)));
    requireSuccess(response.statusCode());
  }

  private void remember(int id, Schema schema) {
    synchronized (cache) {
      cache.put(id, schema);
      if (cache.size() > 128) cache.remove(cache.keySet().iterator().next());
    }
  }

  private record Response(int statusCode, String body) {}

  private Response request(String method, String path, String body) {
    var subscriber = new BoundedRegistryBody(MAX_RESPONSE_BYTES);
    CompletableFuture<HttpResponse<byte[]>> exchange = null;
    try {
      var builder =
          HttpRequest.newBuilder(URI.create(base.toString().replaceAll("/+$", "") + path))
              .timeout(REQUEST_TIMEOUT)
              .header("Content-Type", "application/vnd.schemaregistry.v1+json");
      if (authorization != null && !authorization.isBlank())
        builder.header("Authorization", authorization);
      builder.method(
          method,
          body == null
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofString(body));
      // The response future completes only when the bounded subscriber has the whole body.
      // Waiting on that future bounds headers AND body, including a stalled chunked response.
      exchange = http.sendAsync(builder.build(), info -> subscriber);
      var response = exchange.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      return new Response(
          response.statusCode(), new String(response.body(), StandardCharsets.UTF_8));
    } catch (InterruptedException interrupted) {
      subscriber.cancel();
      if (exchange != null) exchange.cancel(true);
      Thread.currentThread().interrupt();
      throw new Unavailable(interrupted);
    } catch (TimeoutException timeout) {
      subscriber.cancel();
      if (exchange != null) exchange.cancel(true);
      throw new Unavailable(new IOException("Registry request timed out", timeout));
    } catch (ExecutionException failure) {
      subscriber.cancel();
      if (exchange != null) exchange.cancel(true);
      throw new Unavailable(failure.getCause());
    }
  }

  private static void requireSuccess(int status) {
    if (status < 200 || status >= 300)
      throw new Unavailable(new IOException("Registry HTTP status " + status));
  }
}
