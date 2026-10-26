package dev.sharad.telemetry.service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.web.server.SecurityWebFilterChain;

@Configuration
public class SecurityConfiguration {
    private static final Pattern TENANT = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    @Bean
    SecurityWebFilterChain apiSecurity(ServerHttpSecurity http) {
        return http.csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/actuator/health/**").permitAll()
                        .pathMatchers(HttpMethod.POST, "/v1/events").hasAuthority("SCOPE_telemetry.write")
                        .pathMatchers(HttpMethod.GET, "/v1/events/**").hasAuthority("SCOPE_telemetry.read")
                        .pathMatchers("/actuator/prometheus").hasAuthority("SCOPE_telemetry.observe")
                        .anyExchange().denyAll())
                .oauth2ResourceServer(resource -> resource.jwt(Customizer.withDefaults())).build();
    }

    @Bean
    ReactiveJwtDecoder jwtDecoder(@Value("${telemetry.security.mode:issuer}") String mode,
            @Value("${telemetry.security.issuer:}") String issuer,
            @Value("${telemetry.security.audience:telemetry}") String audience,
            @Value("${telemetry.security.demo-secret:}") String secret) {
        if (issuer.isBlank()) throw new IllegalArgumentException("JWT issuer is required");
        NimbusReactiveJwtDecoder decoder;
        if ("demo".equals(mode)) {
            if (secret.getBytes(StandardCharsets.UTF_8).length < 32) {
                throw new IllegalArgumentException("Demo JWT key must be at least 32 bytes");
            }
            decoder = NimbusReactiveJwtDecoder.withSecretKey(
                    new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
                    .macAlgorithm(MacAlgorithm.HS256).build();
        } else if ("issuer".equals(mode)) {
            decoder = NimbusReactiveJwtDecoder.withIssuerLocation(issuer).build();
        } else {
            throw new IllegalArgumentException("Unknown JWT verification mode");
        }
        var audienceValidator = new JwtClaimValidator<Object>("aud",
                values -> values instanceof List<?> list && list.contains(audience));
        var tenantValidator = new JwtClaimValidator<Object>("tenant_id",
                tenant -> tenant instanceof String text && TENANT.matcher(text).matches());
        var expiryRequired = new JwtClaimValidator<Object>("exp", expiry -> expiry instanceof Instant);
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<Jwt>(
                JwtValidators.createDefaultWithIssuer(issuer), audienceValidator, tenantValidator, expiryRequired));
        return decoder;
    }
}
