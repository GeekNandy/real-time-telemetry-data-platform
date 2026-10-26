package dev.sharad.telemetry.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ProductionPolicyTest {
  @Test
  void acceptsVerifiedProductionConnections() {
    assertDoesNotThrow(
        () ->
            ProductionPolicy.validate(
                true,
                "issuer",
                "https://identity.test",
                "https://schemas.test",
                "SASL_SSL",
                "jdbc:postgresql://db.test/data?sslmode=verify-full",
                true));
  }

  @Test
  void rejectsDemoAuthentication() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ProductionPolicy.validate(
                true,
                "demo",
                "https://identity.test",
                "https://schemas.test",
                "SASL_SSL",
                "jdbc:postgresql://db.test/data?sslmode=verify-full",
                true));
  }

  @Test
  void rejectsPlaintextTransport() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ProductionPolicy.validate(
                true,
                "issuer",
                "https://identity.test",
                "http://schemas.test",
                "SASL_SSL",
                "jdbc:postgresql://db.test/data?sslmode=verify-full",
                true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ProductionPolicy.validate(
                true,
                "issuer",
                "https://identity.test",
                "https://schemas.test",
                "PLAINTEXT",
                "jdbc:postgresql://db.test/data?sslmode=verify-full",
                true));
  }

  @Test
  void rejectsAmbiguousOrUnverifiedDatabaseSslSettings() {
    for (String query :
        new String[] {
          "", "?sslmode=require", "?sslmode=verify-full&sslmode=disable", "?notsslmode=verify-full"
        })
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ProductionPolicy.validate(
                  true,
                  "issuer",
                  "https://identity.test",
                  "https://schemas.test",
                  "SSL",
                  "jdbc:postgresql://db.test/data" + query,
                  true));
  }

  @Test
  void collectorDoesNotRequireADatabaseConnection() {
    assertDoesNotThrow(
        () ->
            ProductionPolicy.validate(
                true, "issuer", "https://identity.test", "https://schemas.test", "SSL", "", false));
  }
}
