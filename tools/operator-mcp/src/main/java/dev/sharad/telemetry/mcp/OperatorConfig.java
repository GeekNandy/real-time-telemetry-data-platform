package dev.sharad.telemetry.mcp;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Startup-only destinations; tool arguments cannot supply URLs or file paths. */
record OperatorConfig(URI collector, URI worker, Path tokenFile) {
  OperatorConfig {
    validate(collector);
    validate(worker);
    if (tokenFile != null && !tokenFile.isAbsolute()) {
      throw new IllegalArgumentException("INVALID_CONFIGURATION");
    }
  }

  static OperatorConfig from(Map<String, String> env) {
    String token = env.get("TELEMETRY_OBSERVE_TOKEN_FILE");
    return new OperatorConfig(
        URI.create(env.getOrDefault("TELEMETRY_COLLECTOR_URL", "http://127.0.0.1:8080")),
        URI.create(env.getOrDefault("TELEMETRY_WORKER_URL", "http://127.0.0.1:8082")),
        token == null || token.isBlank() ? null : Path.of(token));
  }

  private static void validate(URI uri) {
    String scheme = uri.getScheme();
    String host = uri.getHost();
    if (host == null
        || uri.getRawUserInfo() != null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null
        || !(uri.getRawPath().isEmpty() || uri.getRawPath().equals("/"))
        || !("https".equals(scheme)
            || ("http".equals(scheme)
                && Set.of("localhost", "127.0.0.1", "[::1]").contains(host)))) {
      throw new IllegalArgumentException("INVALID_CONFIGURATION");
    }
  }
}
