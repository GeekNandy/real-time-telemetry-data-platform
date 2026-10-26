package dev.sharad.telemetry.config;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Production mode refuses demo authentication and plaintext dependency connections. */
public final class ProductionPolicy {
  private ProductionPolicy() {}

  public static void validate(
      boolean production,
      String jwtMode,
      String issuer,
      String registry,
      String kafkaProtocol,
      String databaseUrl,
      boolean usesDatabase) {
    if (!production) return;
    if (!"issuer".equals(jwtMode))
      throw new IllegalArgumentException("Production requires external issuer authentication");
    requireHttps(issuer, "JWT issuer");
    requireHttps(registry, "schema registry");
    if (!List.of("SSL", "SASL_SSL").contains(kafkaProtocol))
      throw new IllegalArgumentException("Production Kafka requires TLS");
    if (usesDatabase) {
      if (databaseUrl == null || !databaseUrl.startsWith("jdbc:postgresql:"))
        throw new IllegalArgumentException("Production database URL");
      String query = URI.create(databaseUrl.substring(5)).getRawQuery();
      var modes = new ArrayList<String>();
      if (query != null)
        for (String parameter : query.split("&")) {
          String[] pair = parameter.split("=", 2);
          if (URLDecoder.decode(pair[0], StandardCharsets.UTF_8).equals("sslmode")
              && pair.length == 2) modes.add(URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
        }
      if (modes.size() != 1 || !"verify-full".equals(modes.getFirst()))
        throw new IllegalArgumentException("Production PostgreSQL requires sslmode=verify-full");
    }
  }

  private static void requireHttps(String value, String name) {
    URI uri = URI.create(value);
    if (!"https".equals(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null)
      throw new IllegalArgumentException("Production requires HTTPS for " + name);
  }
}
