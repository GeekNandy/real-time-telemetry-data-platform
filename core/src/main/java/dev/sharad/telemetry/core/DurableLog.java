package dev.sharad.telemetry.core;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface DurableLog {
  record Position(String stream, int partition, long offset) {
    public Position {
      if (stream == null || stream.isBlank() || partition < 0 || offset < 0) {
        throw new IllegalArgumentException("Invalid durable-log position");
      }
    }
  }

  /** Completion means the configured durable append policy was satisfied. */
  CompletionStage<Position> append(Submission submission) throws Exception;
}
