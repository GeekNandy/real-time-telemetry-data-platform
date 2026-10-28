package dev.sharad.telemetry.service;

import io.netty.channel.ChannelOption;
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/** Bounded, same-origin by default transport for public discovery/JWKS data. */
final class IdentityHttpClient implements AutoCloseable {
  static final Duration DEADLINE = Duration.ofSeconds(3);
  private final ConnectionProvider connections;
  private final WebClient client;

  IdentityHttpClient(String issuer, String additionalOrigins) {
    connections =
        ConnectionProvider.builder("telemetry-identity")
            .maxConnections(4)
            .pendingAcquireMaxCount(8)
            .pendingAcquireTimeout(Duration.ofMillis(500))
            .build();
    try {
      client =
          build(
              issuer,
              additionalOrigins,
              HttpClient.create(connections).option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000),
              DEADLINE);
    } catch (RuntimeException invalid) {
      connections.dispose();
      throw invalid;
    }
  }

  static WebClient build(
      String issuer, String additionalOrigins, HttpClient transport, Duration deadline) {
    URI issuerUri = uri(issuer);
    var origins = new HashSet<String>();
    origins.add(origin(issuerUri));
    if (!additionalOrigins.isBlank())
      for (String value : additionalOrigins.split(",", -1)) {
        URI allowed = uri(value.trim());
        if (allowed.getRawPath() != null
            && !allowed.getRawPath().isEmpty()
            && !"/".equals(allowed.getRawPath()))
          throw new IllegalArgumentException("Trusted JWKS origin must not contain a path");
        origins.add(origin(allowed));
      }
    Set<String> trusted = Set.copyOf(origins);
    if (deadline.isNegative() || deadline.isZero() || deadline.compareTo(DEADLINE) > 0)
      throw new IllegalArgumentException("Invalid identity response deadline");
    return WebClient.builder()
        .clientConnector(
            new ReactorClientHttpConnector(
                transport.followRedirect(false).responseTimeout(deadline)))
        .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(65536))
        .filter(
            (request, next) ->
                Mono.defer(
                    () -> {
                      if (request.method() != HttpMethod.GET
                          || !trusted.contains(origin(request.url())))
                        return Mono.error(
                            new IllegalArgumentException("Untrusted identity endpoint"));
                      long started = System.nanoTime();
                      return next.exchange(request)
                          .timeout(deadline)
                          .map(
                              response -> {
                                long remaining = deadline.toNanos() - (System.nanoTime() - started);
                                Mono<Long> stop =
                                    Mono.delay(Duration.ofNanos(Math.max(0, remaining)))
                                        .flatMap(
                                            ignored ->
                                                Mono.error(
                                                    new TimeoutException(
                                                        "Identity response deadline exceeded")));
                                return response
                                    .mutate()
                                    .body(
                                        body ->
                                            body.takeUntilOther(stop)
                                                .doOnDiscard(
                                                    DataBuffer.class, DataBufferUtils::release))
                                    .build();
                              });
                    }))
        .build();
  }

  private static URI uri(String value) {
    try {
      URI uri = URI.create(value);
      origin(uri);
      return uri;
    } catch (RuntimeException invalid) {
      throw new IllegalArgumentException(
          "Identity endpoints require HTTPS without credentials, query or fragment");
    }
  }

  private static String origin(URI uri) {
    if (!"https".equalsIgnoreCase(uri.getScheme())
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getQuery() != null
        || uri.getFragment() != null
        || uri.getPort() == 0
        || uri.getPort() > 65535)
      throw new IllegalArgumentException(
          "Identity endpoints require HTTPS without credentials, query or fragment");
    return "https://"
        + uri.getHost().toLowerCase(Locale.ROOT)
        + ":"
        + (uri.getPort() == -1 ? 443 : uri.getPort());
  }

  WebClient client() {
    return client;
  }

  @Override
  public void close() {
    connections.dispose();
  }
}
