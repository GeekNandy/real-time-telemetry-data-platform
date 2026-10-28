package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.*;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import tools.jackson.databind.json.JsonMapper;

class OidcHttpsContractTest {
  private static final Duration BUDGET = Duration.ofSeconds(5);
  private static Path certificates;
  private static SSLContext serverTls;
  private static X509Certificate certificate;
  private static RSAKey first;
  private static RSAKey second;
  private final JsonMapper json = JsonMapper.builder().build();
  private final AtomicReference<String> mode = new AtomicReference<>("normal");
  private final AtomicReference<RSAKey> currentKey = new AtomicReference<>();
  private final AtomicInteger dripBytes = new AtomicInteger();
  private final AtomicInteger jwksRequests = new AtomicInteger();
  private HttpsServer peer;
  private java.util.concurrent.ExecutorService executor;
  private ConnectionProvider connections;

  @BeforeAll
  static void certificates() throws Exception {
    certificates = Files.createTempDirectory("synthetic-oidc-tls-");
    Path store = certificates.resolve("server.p12");
    String password = java.util.UUID.randomUUID().toString();
    var command =
        new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
            "-genkeypair",
            "-alias",
            "fixture",
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-dname",
            "CN=localhost",
            "-ext",
            "SAN=dns:localhost",
            "-validity",
            "2",
            "-storetype",
            "PKCS12",
            "-keystore",
            store.toString(),
            "-storepass:env",
            "OIDC_FIXTURE_PASSWORD",
            "-noprompt");
    command.environment().put("OIDC_FIXTURE_PASSWORD", password);
    var process =
        command
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start();
    assertTrue(process.waitFor(20, TimeUnit.SECONDS));
    assertEquals(0, process.exitValue());
    var keys = KeyStore.getInstance("PKCS12");
    try (var input = Files.newInputStream(store)) {
      keys.load(input, password.toCharArray());
    }
    var managers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    managers.init(keys, password.toCharArray());
    serverTls = SSLContext.getInstance("TLS");
    serverTls.init(managers.getKeyManagers(), null, null);
    certificate = (X509Certificate) keys.getCertificate("fixture");
    first = new RSAKeyGenerator(2048).keyID("first").generate();
    second = new RSAKeyGenerator(2048).keyID("second").generate();
  }

  @AfterAll
  static void removeCertificates() throws Exception {
    try (var paths = Files.walk(certificates)) {
      for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
    }
  }

  @BeforeEach
  void startPeer() throws Exception {
    currentKey.set(first);
    peer = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 8);
    peer.setHttpsConfigurator(new HttpsConfigurator(serverTls));
    executor = Executors.newFixedThreadPool(4, Thread.ofVirtual().factory());
    peer.setExecutor(executor);
    peer.createContext("/", this::respond);
    peer.start();
    connections =
        ConnectionProvider.builder("identity-fixture")
            .maxConnections(4)
            .pendingAcquireMaxCount(8)
            .build();
  }

  @AfterEach
  void stopPeer() throws Exception {
    peer.stop(0);
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    connections.dispose();
  }

  private String issuer() {
    return "https://localhost:" + peer.getAddress().getPort();
  }

  private void respond(HttpExchange exchange) throws IOException {
    try (exchange) {
      String path = exchange.getRequestURI().getPath();
      if (path.equals("/jwks")) jwksRequests.incrementAndGet();
      if (mode.get().equals("redirect")) {
        exchange.getResponseHeaders().add("Location", issuer() + "/jwks");
        exchange.sendResponseHeaders(302, -1);
        return;
      }
      String body;
      if (path.equals("/.well-known/openid-configuration")) {
        body =
            json.writeValueAsString(
                Map.of(
                    "issuer",
                    mode.get().equals("metadata-issuer") ? "https://foreign.invalid" : issuer(),
                    "jwks_uri",
                    mode.get().equals("foreign-jwks")
                        ? "https://foreign.invalid/jwks"
                        : issuer() + "/jwks",
                    "id_token_signing_alg_values_supported",
                    List.of("RS256")));
      } else if (path.equals("/jwks")) body = new JWKSet(currentKey.get().toPublicJWK()).toString();
      else {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      if (mode.get().equals("oversized")) body = " ".repeat(70000) + body;
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, bytes.length);
      if (mode.get().equals("drip")) {
        for (int i = 0; i < 20; i++) {
          exchange.getResponseBody().write(' ');
          dripBytes.incrementAndGet();
          exchange.getResponseBody().flush();
          Thread.sleep(80);
        }
      }
      exchange.getResponseBody().write(bytes);
    } catch (InterruptedException stopped) {
      Thread.currentThread().interrupt();
    } catch (IOException disconnected) {
      /* Expected when bounded clients cancel negative fixtures. */
    }
  }

  private HttpClient trustedTransport() throws Exception {
    var ssl = SslContextBuilder.forClient().trustManager(certificate).build();
    return HttpClient.create(connections)
        .secure(spec -> spec.sslContext(ssl).handshakeTimeout(Duration.ofSeconds(2)));
  }

  private WebClient trustedClient(Duration deadline) throws Exception {
    return IdentityHttpClient.build(issuer(), "", trustedTransport(), deadline);
  }

  private ReactiveJwtDecoder decoder(WebClient client) {
    return SecurityConfiguration.createDecoder("issuer", issuer(), "telemetry", "", client);
  }

  private JWTClaimsSet.Builder claims() {
    return new JWTClaimsSet.Builder()
        .issuer(issuer())
        .audience("telemetry")
        .subject("synthetic")
        .claim("tenant_id", "tenant-a")
        .claim("scope", "telemetry.read telemetry.write")
        .issueTime(new Date())
        .expirationTime(Date.from(Instant.now().plusSeconds(120)));
  }

  private String token(RSAKey key, JWTClaimsSet claims) throws Exception {
    var jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256)
                .type(JOSEObjectType.JWT)
                .keyID(key.getKeyID())
                .build(),
            claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  private void reject(ReactiveJwtDecoder decoder, String token) {
    assertThrows(RuntimeException.class, () -> decoder.decode(token).block(BUDGET));
  }

  @Test
  void verifiesRealHttpsDiscoveryAndSignatureThenRotatesKeys() throws Exception {
    var decoder = decoder(trustedClient(IdentityHttpClient.DEADLINE));
    assertEquals(
        "tenant-a",
        decoder.decode(token(first, claims().build())).block(BUDGET).getClaimAsString("tenant_id"));
    int before = jwksRequests.get();
    currentKey.set(second);
    assertEquals(
        "tenant-a",
        decoder
            .decode(token(second, claims().build()))
            .block(BUDGET)
            .getClaimAsString("tenant_id"));
    assertTrue(jwksRequests.get() > before);
    reject(decoder, token(new RSAKeyGenerator(2048).keyID("unknown").generate(), claims().build()));
  }

  @Test
  void validatesIssuerAudienceTenantExpiryAndNotBeforeWithRealRsaTokens() throws Exception {
    var decoder = decoder(trustedClient(IdentityHttpClient.DEADLINE));
    for (var invalid :
        List.of(
            claims().issuer("https://foreign.invalid").build(),
            claims().audience("foreign").build(),
            claims().claim("tenant_id", "../foreign").build(),
            claims().claim("tenant_id", null).build(),
            claims().expirationTime(null).build(),
            claims().expirationTime(Date.from(Instant.now().minusSeconds(600))).build(),
            claims().notBeforeTime(Date.from(Instant.now().plusSeconds(600))).build()))
      reject(decoder, token(first, invalid));
    reject(decoder, "x".repeat(8193));
  }

  @Test
  void rejectsUntrustedCertificateAndWrongTlsHostname() throws Exception {
    var untrusted =
        IdentityHttpClient.build(
            issuer(), "", HttpClient.create(connections), IdentityHttpClient.DEADLINE);
    reject(decoder(untrusted), token(first, claims().build()));
    var trusted = trustedClient(IdentityHttpClient.DEADLINE);
    String wrongHost = "https://127.0.0.1:" + peer.getAddress().getPort();
    // Permit the IP origin explicitly, so rejection must come from TLS SAN verification.
    trusted =
        IdentityHttpClient.build(
            issuer(), wrongHost, trustedTransport(), IdentityHttpClient.DEADLINE);
    var finalClient = trusted;
    assertThrows(
        RuntimeException.class,
        () ->
            finalClient
                .get()
                .uri(wrongHost + "/jwks")
                .retrieve()
                .bodyToMono(String.class)
                .block(BUDGET));
  }

  @Test
  void refusesRedirectsForeignJwksAndMismatchedDiscoveryIssuer() throws Exception {
    for (String value : List.of("redirect", "foreign-jwks", "metadata-issuer")) {
      mode.set(value);
      reject(decoder(trustedClient(IdentityHttpClient.DEADLINE)), token(first, claims().build()));
    }
  }

  @Test
  void realSignedTokensEnforceReadWriteObserveScopesOverActualHttp() throws Exception {
    var decoder = decoder(trustedClient(IdentityHttpClient.DEADLINE));
    var httpSecurity =
        org.springframework.security.config.web.server.ServerHttpSecurity.http()
            .oauth2ResourceServer(resource -> resource.jwt(jwt -> jwt.jwtDecoder(decoder)));
    var chain = new SecurityConfiguration().apiSecurity(httpSecurity);
    var router =
        org.springframework.web.reactive.function.server.RouterFunctions.route()
            .GET(
                "/v1/events/fixture",
                r ->
                    org.springframework.web.reactive.function.server.ServerResponse.ok()
                        .bodyValue("synthetic"))
            .POST(
                "/v1/events",
                r ->
                    org.springframework.web.reactive.function.server.ServerResponse.accepted()
                        .build())
            .GET(
                "/actuator/prometheus",
                r ->
                    org.springframework.web.reactive.function.server.ServerResponse.ok()
                        .bodyValue("bounded_metric 1"))
            .GET(
                "/actuator/health",
                r -> org.springframework.web.reactive.function.server.ServerResponse.ok().build())
            .build();
    var handler =
        org.springframework.web.server.adapter.WebHttpHandlerBuilder.webHandler(
                org.springframework.web.reactive.function.server.RouterFunctions.toWebHandler(
                    router))
            .filter(new org.springframework.security.web.server.WebFilterChainProxy(chain))
            .build();
    var server =
        reactor.netty.http.server.HttpServer.create()
            .host("127.0.0.1")
            .port(0)
            .handle(new org.springframework.http.server.reactive.ReactorHttpHandlerAdapter(handler))
            .bindNow();
    try (var client = java.net.http.HttpClient.newHttpClient()) {
      String base = "http://127.0.0.1:" + server.port();
      String read = token(first, claims().claim("scope", "telemetry.read").build());
      String write = token(first, claims().claim("scope", "telemetry.write").build());
      String observe = token(first, claims().claim("scope", "telemetry.observe").build());
      for (var item :
          List.of(
              List.of("GET", "/v1/events/fixture", read, "200"),
              List.of("GET", "/v1/events/fixture", write, "403"),
              List.of("POST", "/v1/events", read, "403"),
              List.of("POST", "/v1/events", write, "202"),
              List.of("GET", "/actuator/prometheus", write, "403"),
              List.of("GET", "/actuator/prometheus", observe, "200"),
              List.of("GET", "/v1/events/fixture", "", "401"),
              List.of("GET", "/actuator/health", "", "200"))) {
        var request =
            java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + item.get(1)))
                .timeout(BUDGET)
                .method(item.getFirst(), java.net.http.HttpRequest.BodyPublishers.noBody());
        if (!item.get(2).isEmpty()) request.header("Authorization", "Bearer " + item.get(2));
        var response =
            client.send(request.build(), java.net.http.HttpResponse.BodyHandlers.discarding());
        assertEquals(Integer.parseInt(item.get(3)), response.statusCode());
        assertTrue(
            response.headers().allValues("cache-control").stream()
                .anyMatch(value -> value.contains("no-store")));
      }
    } finally {
      server.disposeNow();
    }
  }

  @Test
  void boundsWholeBodyEvenWhenBytesKeepArriving() throws Exception {
    mode.set("drip");
    long started = System.nanoTime();
    reject(decoder(trustedClient(Duration.ofMillis(800))), token(first, claims().build()));
    assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(3)) < 0);
    assertTrue(
        dripBytes.get() >= 2, "Response bytes must arrive before absolute body deadline expires");
  }

  @Test
  void boundsDecodedResponseMemoryAndRejectsUnsafeOrigins() throws Exception {
    mode.set("oversized");
    reject(decoder(trustedClient(IdentityHttpClient.DEADLINE)), token(first, claims().build()));
    for (String value :
        List.of(
            "http://localhost",
            "https://user:password@localhost",
            "https://localhost/?secret=x",
            "https://localhost/#x"))
      assertThrows(
          IllegalArgumentException.class,
          () ->
              IdentityHttpClient.build(
                  value, "", HttpClient.create(), IdentityHttpClient.DEADLINE));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IdentityHttpClient.build(
                issuer(),
                "https://other.invalid/path",
                HttpClient.create(),
                IdentityHttpClient.DEADLINE));
  }
}
