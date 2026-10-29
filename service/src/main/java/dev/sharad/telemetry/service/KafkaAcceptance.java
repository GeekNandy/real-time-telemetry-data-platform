package dev.sharad.telemetry.service;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.core.DurableLog;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded append intents and HTTP waiters; cancellation never discards durable spool work. */
final class KafkaAcceptance implements AutoCloseable {
  private static final int BATCH_RECORDS = 32;
  private static final long BATCH_BYTES = 1_048_576;
  private static final long QUEUED_BYTES = DiskBuffer.MAX_BATCH_BYTES;
  private static final long COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  record Confirmed(long sequence, DurableLog.Position position) {
    Confirmed {
      if (sequence < 1) throw new IllegalArgumentException("Invalid confirmed sequence");
      java.util.Objects.requireNonNull(position);
    }
  }

  private static final class Waiter {
    final CompletableFuture<DurableLog.Position> result = new CompletableFuture<>();
    long sequence;
    ScheduledFuture<?> timeout;
  }

  private record Write(DiskBuffer.Append record, Waiter waiter, long began, long charge) {}

  @FunctionalInterface
  interface Appender {
    void append(
        List<DiskBuffer.Append> records,
        java.util.function.Consumer<List<DiskBuffer.Entry>> beforeVisible)
        throws IOException;
  }

  private final DiskBuffer buffer;
  private final Appender appender;
  private final int capacity;
  private final long deadlineNanos;
  private final Map<Long, Waiter> waiting = new HashMap<>();
  private final Set<Waiter> active = new HashSet<>();
  private final ArrayDeque<Write> queued = new ArrayDeque<>();
  private final ScheduledThreadPoolExecutor deadlines;
  private final Thread writer;
  private final Timer appendDuration;
  private final DistributionSummary batchSize;
  private long queuedBytes;
  private int pendingWrites;
  private boolean closed;

  KafkaAcceptance(DiskBuffer buffer, int capacity, Duration deadline) {
    this(buffer, capacity, deadline, new SimpleMeterRegistry());
  }

  KafkaAcceptance(DiskBuffer buffer, int capacity, Duration deadline, MeterRegistry meters) {
    this(buffer, capacity, deadline, meters, buffer::appendBatch);
  }

  KafkaAcceptance(
      DiskBuffer buffer, int capacity, Duration deadline, MeterRegistry meters, Appender appender) {
    if (capacity < 1
        || capacity > 100_000
        || deadline == null
        || deadline.isNegative()
        || deadline.isZero()
        || deadline.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("Invalid Kafka acceptance limits");
    this.buffer = java.util.Objects.requireNonNull(buffer);
    this.appender = java.util.Objects.requireNonNull(appender);
    this.capacity = capacity;
    deadlineNanos = deadline.toNanos();
    appendDuration =
        Timer.builder("telemetry.spool.append.duration")
            .minimumExpectedValue(Duration.ofNanos(100_000))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentileHistogram()
            .register(meters);
    batchSize =
        DistributionSummary.builder("telemetry.spool.append.batch.size")
            .minimumExpectedValue(1.0)
            .maximumExpectedValue((double) BATCH_RECORDS)
            .register(meters);
    meters.gauge("telemetry.spool.pending.writes", this, value -> value.pendingWrites());
    meters.gauge("telemetry.spool.pending.bytes", this, value -> value.pendingBytes());
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
    writer = new Thread(this::writeLoop, "telemetry-spool-append");
    writer.setDaemon(true);
    writer.start();
  }

  CompletionStage<DurableLog.Position> append(
      String key, byte[] payload, Map<String, String> traceHeaders) throws IOException {
    long began = System.nanoTime();
    if (payload == null || payload.length > buffer.maxRecordBytes())
      throw new IllegalArgumentException("Payload exceeds record limit");
    var record = new DiskBuffer.Append(key, payload, traceHeaders);
    buffer.validateAppend(record);
    // The fixed allowance covers bounded key/trace metadata and framing without a second encode.
    long charge = payload.length + 2048L;
    synchronized (this) {
      if (closed) throw new IOException("Collector acceptance is closed");
      if (active.size() >= capacity
          || pendingWrites >= capacity
          || charge > QUEUED_BYTES - queuedBytes)
        throw new RejectedExecutionException("Kafka acceptance capacity exhausted");
      var waiter = new Waiter();
      active.add(waiter);
      queued.addLast(new Write(record, waiter, began, charge));
      queuedBytes += charge;
      pendingWrites++;
      waiter.timeout =
          deadlines.schedule(
              () -> expire(waiter),
              Math.max(0, deadlineNanos - (System.nanoTime() - began)),
              TimeUnit.NANOSECONDS);
      notifyAll();
      return waiter.result.minimalCompletionStage();
    }
  }

  private List<Write> takeBatch() throws InterruptedException {
    synchronized (this) {
      while (queued.isEmpty() && !closed) wait();
      if (closed) return List.of();
      long until = queued.getFirst().began() + COALESCE_NANOS;
      // Fill one small group until its absolute deadline or record bound. A backlog drains at once.
      while (queued.size() < BATCH_RECORDS && !closed) {
        long remaining = until - System.nanoTime();
        if (remaining <= 0) break;
        TimeUnit.NANOSECONDS.timedWait(this, remaining);
      }
      if (closed) return List.of();
      var batch = new ArrayList<Write>();
      long bytes = 0;
      while (!queued.isEmpty() && batch.size() < BATCH_RECORDS) {
        var head = queued.getFirst();
        if (!batch.isEmpty() && head.charge() > BATCH_BYTES - bytes) break;
        batch.add(queued.removeFirst());
        bytes += head.charge();
      }
      return List.copyOf(batch);
    }
  }

  private void writeLoop() {
    try {
      for (; ; ) {
        var batch = takeBatch();
        if (batch.isEmpty()) return;
        persist(batch);
      }
    } catch (InterruptedException stopped) {
      Thread.currentThread().interrupt();
      close();
    }
  }

  private void persist(List<Write> batch) {
    Exception failure = null;
    try {
      appender.append(
          batch.stream().map(Write::record).toList(),
          entries -> {
            if (entries.size() != batch.size())
              throw new IllegalStateException("Incomplete append registration");
            synchronized (this) {
              for (int index = 0; index < batch.size(); index++) {
                var waiter = batch.get(index).waiter();
                if (active.contains(waiter)) {
                  waiter.sequence = entries.get(index).sequence();
                  waiting.put(waiter.sequence, waiter);
                }
              }
            }
          });
    } catch (IOException | RuntimeException error) {
      failure = error;
    } finally {
      batchSize.record(batch.size());
      synchronized (this) {
        for (var write : batch) {
          pendingWrites--;
          queuedBytes -= write.charge();
          if (failure != null && active.remove(write.waiter())) {
            waiting.remove(write.waiter().sequence, write.waiter());
            write.waiter().timeout.cancel(false);
          }
        }
      }
      for (var write : batch) {
        appendDuration.record(System.nanoTime() - write.began(), TimeUnit.NANOSECONDS);
        if (failure != null) write.waiter().result.completeExceptionally(failure);
      }
    }
  }

  /** Call only after producer acknowledgement; checkpoint before allowing an HTTP 202. */
  void confirm(long sequence, DurableLog.Position position) throws IOException {
    confirmBatch(List.of(new Confirmed(sequence, position)));
  }

  /** All positions must be producer-confirmed before checkpointing any entry in this prefix. */
  void confirmBatch(List<Confirmed> confirmed) throws IOException {
    if (confirmed == null || confirmed.isEmpty() || confirmed.size() > DiskBuffer.MAX_BATCH_RECORDS)
      throw new IllegalArgumentException("Invalid confirmation batch");
    confirmed = List.copyOf(confirmed);
    var settled = new ArrayList<Map.Entry<Waiter, DurableLog.Position>>();
    buffer.acknowledgeBatch(confirmed.stream().map(Confirmed::sequence).toList());
    synchronized (this) {
      for (var item : confirmed) {
        Waiter waiter = waiting.remove(item.sequence());
        if (waiter != null) {
          active.remove(waiter);
          waiter.timeout.cancel(false);
          settled.add(Map.entry(waiter, item.position()));
        }
      }
    }
    // Subscriber callbacks run outside the state monitor and filesystem protocol.
    for (var item : settled) item.getKey().result.complete(item.getValue());
  }

  private void expire(Waiter expected) {
    boolean removed;
    synchronized (this) {
      removed = active.remove(expected);
      if (expected.sequence != 0) waiting.remove(expected.sequence, expected);
    }
    if (removed)
      expected.result.completeExceptionally(
          new TimeoutException("Kafka acknowledgement unconfirmed"));
  }

  synchronized int pendingWaiters() {
    return active.size();
  }

  synchronized int pendingWrites() {
    return pendingWrites;
  }

  synchronized long pendingBytes() {
    return queuedBytes;
  }

  int scheduledWaiters() {
    return deadlines.getQueue().size();
  }

  @Override
  public void close() {
    java.util.List<Waiter> abandoned;
    synchronized (this) {
      if (closed) return;
      closed = true;
      abandoned = List.copyOf(active);
      active.clear();
      waiting.clear();
      for (var write : queued) {
        pendingWrites--;
        queuedBytes -= write.charge();
      }
      queued.clear();
      for (var waiter : abandoned) waiter.timeout.cancel(false);
      deadlines.shutdown();
      notifyAll();
    }
    for (var waiter : abandoned)
      waiter.result.completeExceptionally(new IOException("Collector stopped before confirmation"));
    writer.interrupt();
    if (Thread.currentThread() != writer) {
      try {
        writer.join(10_000);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      if (writer.isAlive())
        throw new IllegalStateException("Spool writer did not stop within its bound");
    }
  }
}
