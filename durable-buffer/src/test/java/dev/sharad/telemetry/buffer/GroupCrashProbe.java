package dev.sharad.telemetry.buffer;

import dev.sharad.telemetry.config.PlatformSettings;
import java.nio.file.Path;
import java.util.List;

/** Child process intentionally kept open so the parent can exercise an ungraceful exit. */
public final class GroupCrashProbe {
  private GroupCrashProbe() {}

  public static void main(String[] args) throws Exception {
    try (var buffer =
        new DiskBuffer(Path.of(args[0]), new PlatformSettings.Buffer(1_048_576, 128, 4096))) {
      var entries =
          buffer.appendBatch(
              List.of(
                  DiskBufferGroupTest.intent("a:one"),
                  DiskBufferGroupTest.intent("a:two"),
                  DiskBufferGroupTest.intent("a:three")));
      System.out.println("COMMITTED " + entries.size());
      System.out.flush();
      System.in.read();
    }
  }
}
