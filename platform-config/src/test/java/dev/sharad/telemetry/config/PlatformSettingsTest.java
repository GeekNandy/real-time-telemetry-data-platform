package dev.sharad.telemetry.config;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class PlatformSettingsTest {
  @Test
  void rejectsInvalidAdmissionRatios() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlatformSettings(
                8,
                9,
                2,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                new PlatformSettings.Buffer(4096, 8, 1024)));
  }

  @Test
  void rejectsUnboundedOrImpossibleBufferLimits() {
    assertThrows(
        IllegalArgumentException.class, () -> new PlatformSettings.Buffer(4096, 100001, 1024));
    assertThrows(IllegalArgumentException.class, () -> new PlatformSettings.Buffer(1024, 8, 2048));
    assertThrows(IllegalArgumentException.class, () -> new PlatformSettings.Buffer(0, 8, 1024));
  }

  @Test
  void rejectsNonpositiveDeadlines() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlatformSettings(
                8,
                4,
                2,
                Duration.ZERO,
                Duration.ofSeconds(10),
                new PlatformSettings.Buffer(4096, 8, 1024)));
  }
}
