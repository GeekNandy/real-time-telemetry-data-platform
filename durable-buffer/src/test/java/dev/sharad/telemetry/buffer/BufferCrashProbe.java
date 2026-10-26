package dev.sharad.telemetry.buffer;

import dev.sharad.telemetry.config.PlatformSettings;
import java.nio.file.Path;
import java.util.Map;

public final class BufferCrashProbe {
  private BufferCrashProbe() {}

  public static void main(String[] args) throws Exception {
    try (var buffer =
        new DiskBuffer(Path.of(args[0]), new PlatformSettings.Buffer(1024 * 1024, 100, 4096))) {
      var entry =
          buffer.append(
              "tenant:crash-probe",
              "accepted-before-kill".getBytes(java.nio.charset.StandardCharsets.UTF_8),
              Map.of());
      System.out.println("ACK " + entry.sequence());
      System.out.flush();
      Thread.sleep(Long.MAX_VALUE);
    }
  }
}
