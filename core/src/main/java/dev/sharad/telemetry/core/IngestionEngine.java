package dev.sharad.telemetry.core;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/** Bounded intake whose acceptance boundary is the configured durable-log completion. */
public final class IngestionEngine implements AutoCloseable {
  public record Acceptance(UUID eventId, String tenantId, DurableLog.Position position) {}

  public record Snapshot(
      AdmissionController.Snapshot admission, long accepted, long failed, long rejected) {}

  private final DurableLog broker;
  private final AdmissionController admission;
  private final ThreadPoolExecutor workers;
  private final long queueDeadlineNanos;
  private final LongAdder accepted = new LongAdder();
  private final LongAdder failed = new LongAdder();
  private final LongAdder rejected = new LongAdder();

  public IngestionEngine(
      DurableLog broker, int globalCapacity, int tenantCapacity, int sendThreads) {
    this(broker, globalCapacity, tenantCapacity, sendThreads, Duration.ofSeconds(2));
  }

  public IngestionEngine(
      DurableLog broker,
      int globalCapacity,
      int tenantCapacity,
      int sendThreads,
      Duration queueDeadline) {
    this.broker = Objects.requireNonNull(broker, "broker");
    if (sendThreads < 1) throw new IllegalArgumentException("sendThreads must be positive");
    this.admission = new AdmissionController(globalCapacity, tenantCapacity);
    if (queueDeadline == null || queueDeadline.isNegative() || queueDeadline.isZero())
      throw new IllegalArgumentException("Queue deadline");
    this.queueDeadlineNanos = queueDeadline.toNanos();
    var sequence = new AtomicInteger();
    this.workers =
        new ThreadPoolExecutor(
            sendThreads,
            sendThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(globalCapacity),
            runnable -> new Thread(runnable, "telemetry-send-" + sequence.incrementAndGet()),
            new ThreadPoolExecutor.AbortPolicy());
  }

  public CompletionStage<Acceptance> accept(Event event) {
    return accept(new Submission(event, java.util.Map.of()));
  }

  public CompletionStage<Acceptance> accept(Submission submission) {
    Event event = submission.event();
    Objects.requireNonNull(event, "event");
    AdmissionController.Lease lease;
    try {
      lease = admission.acquire(event.tenantId());
    } catch (AdmissionController.Rejected error) {
      rejected.increment();
      throw error;
    }
    var result = new CompletableFuture<Acceptance>();
    var settled = new AtomicBoolean();
    var attempt = new Attempt(event, lease, result, settled);
    long enqueued = System.nanoTime();
    try {
      workers.execute(
          () -> {
            try {
              if (System.nanoTime() - enqueued > queueDeadlineNanos)
                throw new TimeoutException("Append queue deadline exceeded");
              CompletionStage<DurableLog.Position> pending =
                  Objects.requireNonNull(broker.append(submission), "durable append completion");
              pending.whenComplete(attempt::finish);
            } catch (Exception error) {
              attempt.finish(null, error);
            }
          });
    } catch (RejectedExecutionException error) {
      attempt.finish(null, error);
    }
    // Consumers can cancel their copy, but cannot cancel the actual broker operation.
    return result.minimalCompletionStage();
  }

  public Snapshot snapshot() {
    return new Snapshot(admission.snapshot(), accepted.sum(), failed.sum(), rejected.sum());
  }

  public CompletionStage<Void> beginDrain() {
    var drained = admission.stopAccepting();
    workers.shutdown();
    return drained;
  }

  /** False means shutdown reached its deadline with unconfirmed operations remaining. */
  public boolean close(Duration deadline) throws InterruptedException {
    if (deadline.isNegative()) throw new IllegalArgumentException("Negative deadline");
    var drained = beginDrain().toCompletableFuture();
    try {
      drained.get(deadline.toNanos(), TimeUnit.NANOSECONDS);
      return true;
    } catch (TimeoutException error) {
      return false;
    } catch (ExecutionException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  @Override
  public void close() {
    try {
      close(Duration.ofSeconds(10));
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
    }
  }

  private final class Attempt {
    private final Event event;
    private final AdmissionController.Lease lease;
    private final CompletableFuture<Acceptance> result;
    private final AtomicBoolean settled;

    private Attempt(
        Event event,
        AdmissionController.Lease lease,
        CompletableFuture<Acceptance> result,
        AtomicBoolean settled) {
      this.event = event;
      this.lease = lease;
      this.result = result;
      this.settled = settled;
    }

    private void finish(DurableLog.Position position, Throwable error) {
      if (!settled.compareAndSet(false, true)) return;
      if (error == null && position == null) {
        error = new IllegalStateException("DurableLog returned no position");
      }
      lease.close();
      if (error == null) {
        accepted.increment();
        result.complete(new Acceptance(event.eventId(), event.tenantId(), position));
      } else {
        failed.increment();
        result.completeExceptionally(new AppendFailure(error));
      }
    }
  }

  public static final class AppendFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public AppendFailure(Throwable cause) {
      super("DurableLog acceptance unconfirmed", cause);
    }
  }
}
