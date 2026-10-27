package dev.sharad.telemetry.service;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.core.DurableLog;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded HTTP waiters; a timeout never removes durable work from the spool. */
final class KafkaAcceptance implements AutoCloseable {
  private static final class Waiter {
    final CompletableFuture<DurableLog.Position> result = new CompletableFuture<>();
    ScheduledFuture<?> timeout;
  }

  private final DiskBuffer buffer;
  private final int capacity;
  private final long deadlineNanos;
  private final Map<Long, Waiter> waiting = new HashMap<>();
  private final ScheduledThreadPoolExecutor deadlines;
  private boolean closed;

  KafkaAcceptance(DiskBuffer buffer, int capacity, Duration deadline) {
    if (capacity < 1
        || capacity > 100_000
        || deadline == null
        || deadline.isNegative()
        || deadline.isZero()
        || deadline.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("Invalid Kafka acceptance limits");
    this.buffer = java.util.Objects.requireNonNull(buffer);
    this.capacity = capacity;
    deadlineNanos = deadline.toNanos();
    deadlines =
        new ScheduledThreadPoolExecutor(
            1,
            task -> {
              var thread = new Thread(task, "telemetry-acceptance-deadlines");
              thread.setDaemon(true);
              return thread;
            });
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  CompletionStage<DurableLog.Position> append(
      String key, byte[] payload, Map<String, String> traceHeaders) throws IOException {
    synchronized (this) {
      if (closed) throw new IOException("Collector acceptance is closed");
      if (waiting.size() >= capacity)
        throw new RejectedExecutionException("Kafka acceptance capacity exhausted");
      long started = System.nanoTime();
      var entry = buffer.append(key, payload, traceHeaders);
      var waiter = new Waiter();
      // Publication cannot settle between append and waiter registration: confirm uses this
      // monitor.
      waiting.put(entry.sequence(), waiter);
      waiter.timeout =
          deadlines.schedule(
              () -> expire(entry.sequence(), waiter),
              Math.max(0, deadlineNanos - (System.nanoTime() - started)),
              TimeUnit.NANOSECONDS);
      return waiter.result.minimalCompletionStage();
    }
  }

  /** Call only after producer acknowledgement; checkpoint before allowing an HTTP 202. */
  void confirm(long sequence, DurableLog.Position position) throws IOException {
    java.util.Objects.requireNonNull(position);
    Waiter waiter;
    synchronized (this) {
      buffer.acknowledge(sequence);
      waiter = waiting.remove(sequence);
      if (waiter != null) waiter.timeout.cancel(false);
    }
    // Completing outside the monitor keeps subscriber callbacks out of the filesystem protocol.
    if (waiter != null) waiter.result.complete(position);
  }

  private void expire(long sequence, Waiter expected) {
    boolean removed;
    synchronized (this) {
      removed = waiting.remove(sequence, expected);
    }
    if (removed)
      expected.result.completeExceptionally(
          new TimeoutException("Kafka acknowledgement unconfirmed"));
  }

  synchronized int pendingWaiters() {
    return waiting.size();
  }

  int scheduledWaiters() {
    return deadlines.getQueue().size();
  }

  @Override
  public void close() {
    java.util.List<Waiter> abandoned;
    synchronized (this) {
      closed = true;
      abandoned = java.util.List.copyOf(waiting.values());
      waiting.clear();
      for (var waiter : abandoned) waiter.timeout.cancel(false);
      deadlines.shutdown();
    }
    for (var waiter : abandoned)
      waiter.result.completeExceptionally(new IOException("Collector stopped before confirmation"));
  }
}
