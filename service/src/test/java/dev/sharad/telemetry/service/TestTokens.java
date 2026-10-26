package dev.sharad.telemetry.service;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;

final class TestTokens {
  static final String SECRET = "test-only-key-with-at-least-thirty-two-bytes";
  static final String ISSUER = "https://issuer.test";

  private TestTokens() {}

  static JWTClaimsSet.Builder claims(String tenant) {
    Instant now = Instant.now();
    return new JWTClaimsSet.Builder()
        .issuer(ISSUER)
        .audience("telemetry")
        .subject("contract-test")
        .claim("tenant_id", tenant)
        .claim("scope", "telemetry.write telemetry.read")
        .issueTime(Date.from(now))
        .expirationTime(Date.from(now.plusSeconds(300)));
  }

  static String sign(JWTClaimsSet claims, String key) throws JOSEException {
    var jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.HS256).type(JOSEObjectType.JWT).build(), claims);
    jwt.sign(new MACSigner(key.getBytes(StandardCharsets.UTF_8)));
    return jwt.serialize();
  }

  static String tenant(String tenant) throws JOSEException {
    return sign(claims(tenant).build(), SECRET);
  }
}
