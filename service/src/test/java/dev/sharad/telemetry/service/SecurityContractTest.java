package dev.sharad.telemetry.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;

class SecurityContractTest {
  private final ReactiveJwtDecoder decoder =
      new SecurityConfiguration()
          .jwtDecoder("demo", TestTokens.ISSUER, "telemetry", TestTokens.SECRET);

  @Test
  void verifiesTheSignatureAndTenant() throws Exception {
    var jwt = decoder.decode(TestTokens.tenant("tenant-a")).block(Duration.ofSeconds(2));
    assertEquals("tenant-a", jwt.getClaimAsString("tenant_id"));
  }

  @Test
  void rejectsAnInvalidSignature() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a").build(),
            "different-test-key-with-at-least-thirty-two-bytes"));
  }

  @Test
  void rejectsExpiredTokens() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a")
                .issueTime(Date.from(Instant.now().minusSeconds(900)))
                .expirationTime(Date.from(Instant.now().minusSeconds(600)))
                .build(),
            TestTokens.SECRET));
  }

  @Test
  void rejectsTokensWithoutExpiry() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a").expirationTime(null).build(), TestTokens.SECRET));
  }

  @Test
  void rejectsWrongIssuer() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a").issuer("https://other.test").build(), TestTokens.SECRET));
  }

  @Test
  void rejectsWrongAudience() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a").audience("other").build(), TestTokens.SECRET));
  }

  @Test
  void rejectsMissingTenant() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a").claim("tenant_id", null).build(), TestTokens.SECRET));
  }

  @Test
  void rejectsInvalidTenant() throws Exception {
    rejects(TestTokens.sign(TestTokens.claims("../tenant-b").build(), TestTokens.SECRET));
  }

  @Test
  void rejectsAnIncorrectTenantClaimType() throws Exception {
    rejects(
        TestTokens.sign(
            TestTokens.claims("tenant-a").claim("tenant_id", 123).build(), TestTokens.SECRET));
  }

  @Test
  void rejectsAnUnsafeDemoKey() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SecurityConfiguration()
                .jwtDecoder("demo", TestTokens.ISSUER, "telemetry", "short"));
  }

  private void rejects(String token) {
    assertThrows(JwtException.class, () -> decoder.decode(token).block(Duration.ofSeconds(2)));
  }
}
