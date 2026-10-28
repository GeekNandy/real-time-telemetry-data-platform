package dev.sharad.telemetry.service;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.core.DurableLog;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;

/** One ordered, bounded publication window per volume; checkpoint only a fully confirmed prefix. */
public final class BufferDispatcher implements SmartLifecycle {
  @FunctionalInterface
  interface Publisher {
    CompletableFuture<DurableLog.Position> send(ProducerRecord<String, byte[]> record);
  }

  private record Pending(
      DiskBuffer.Entry entry, CompletableFuture<DurableLog.Position> publication) {}

  private final DiskBuffer buffer;
  private final KafkaAcceptance acceptance;
  private final Publisher publisher;
  private final PlatformTelemetry telemetry;
  private final String topic;
  private final Counter delivered;
  private final Timer reading;
  private final Timer publication;
  private final Timer checkpoint;
  private final DistributionSummary batchSize;
  private final int batchRecords;
  private final long batchBytes;
  private final long publicationDeadlineNanos;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<String> fatal = new AtomicReference<>();
  private volatile Thread worker;

  public BufferDispatcher(
      DiskBuffer buffer,
      KafkaAcceptance acceptance,
      KafkaTemplate<String, byte[]> kafka,
      PlatformTelemetry telemetry,
      String topic,
      MeterRegistry meters) {
    this(
        buffer, acceptance, kafka, telemetry, topic, meters, 64, 1_048_576, Duration.ofSeconds(10));
  }

  BufferDispatcher(
      DiskBuffer buffer,
      KafkaAcceptance acceptance,
      KafkaTemplate<String, byte[]> kafka,
      PlatformTelemetry telemetry,
      String topic,
      MeterRegistry meters,
      int batchRecords,
      long batchBytes,
      Duration publicationDeadline) {
    this(
        buffer,
        acceptance,
        publisher(kafka),
        telemetry,
        topic,
        meters,
        batchRecords,
        batchBytes,
        publicationDeadline);
  }

  private static Publisher publisher(KafkaTemplate<String, byte[]> kafka) {
    return record ->
        kafka
            .send(record)
            .thenApply(
                result -> {
                  var metadata = result.getRecordMetadata();
                  return new DurableLog.Position(
                      metadata.topic(), metadata.partition(), metadata.offset());
                });
  }

  BufferDispatcher(
      DiskBuffer buffer,
      KafkaAcceptance acceptance,
      Publisher publisher,
      PlatformTelemetry telemetry,
      String topic,
      MeterRegistry meters,
      int batchRecords,
      long batchBytes,
      Duration publicationDeadline) {
    if (batchRecords < 1
        || batchRecords > DiskBuffer.MAX_BATCH_RECORDS
        || batchBytes < 1
        || batchBytes > DiskBuffer.MAX_BATCH_BYTES
        || publicationDeadline == null
        || publicationDeadline.isZero()
        || publicationDeadline.isNegative()
        || publicationDeadline.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("Invalid dispatcher batch limits");
    this.buffer = buffer;
    this.acceptance = acceptance;
    this.publisher = publisher;
    this.telemetry = telemetry;
    this.topic = topic;
    this.batchRecords = batchRecords;
    this.batchBytes = batchBytes;
    this.publicationDeadlineNanos = publicationDeadline.toNanos();
    delivered = Counter.builder("telemetry.buffer.delivered").register(meters);
    reading = phaseTimer(meters, "read");
    publication = phaseTimer(meters, "publication");
    checkpoint = phaseTimer(meters, "checkpoint");
    batchSize =
        DistributionSummary.builder("telemetry.dispatch.batch.size")
            .minimumExpectedValue(1.0)
            .maximumExpectedValue((double) DiskBuffer.MAX_BATCH_RECORDS)
            .register(meters);
  }

  private static Timer phaseTimer(MeterRegistry meters, String phase) {
    return Timer.builder("telemetry.dispatch.phase.duration")
        .tag("phase", phase)
        .minimumExpectedValue(Duration.ofNanos(100_000))
        .maximumExpectedValue(Duration.ofSeconds(30))
        .publishPercentileHistogram()
        .register(meters);
  }

  @Override
  public synchronized void start() {
    if (!running.compareAndSet(false, true)) return;
    worker = new Thread(this::run, "telemetry-buffer-dispatch");
    worker.setDaemon(true);
    worker.start();
  }

  private void run() {
    int failures = 0;
    try {
      while (running.get()) {
        if (!buffer.awaitRecords(Duration.ofMillis(50))) continue;
        // A small, bounded coalescing interval lets concurrent appends share publication/checkpoint
        // work.
        Thread.sleep(2);
        int outcome = dispatchOnce();
        if (outcome >= 0) {
          failures = 0;
        } else {
          failures++;
          Thread.sleep(Math.min(5000L, 200L << Math.min(failures, 4)));
        }
      }
    } catch (InterruptedException stopped) {
      Thread.currentThread().interrupt();
    } catch (IOException corrupt) {
      if (running.get()) fatal.set("buffer-read-failure");
    } finally {
      running.set(false);
    }
  }

  /**
   * A failed publication/checkpoint retains the whole unconfirmed prefix for deduplicated replay.
   */
  int dispatchOnce() throws IOException, InterruptedException {
    long readStarted = System.nanoTime();
    java.util.List<DiskBuffer.Entry> entries;
    try {
      entries = buffer.peekBatch(batchRecords, batchBytes);
    } finally {
      reading.record(System.nanoTime() - readStarted, TimeUnit.NANOSECONDS);
    }
    if (entries.isEmpty()) return 0;
    batchSize.record(entries.size());
    long started = System.nanoTime();
    boolean success = false;
    var spans = new ArrayList<Span>();
    var pending = new ArrayList<Pending>();
    try {
      var confirmed = new ArrayList<KafkaAcceptance.Confirmed>();
      long publishStarted = System.nanoTime();
      try {
        for (var entry : entries) {
          if (System.nanoTime() - publishStarted >= publicationDeadlineNanos)
            throw new TimeoutException("Batch publication deadline exceeded");
          var span =
              telemetry.start(
                  PlatformTelemetry.Stage.DISPATCH, SpanKind.PRODUCER, entry.traceHeaders());
          spans.add(span);
          var record = new ProducerRecord<String, byte[]>(topic, entry.key(), entry.payload());
          telemetry
              .inject(span)
              .forEach(
                  (key, value) ->
                      record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
          pending.add(new Pending(entry, publisher.send(record)));
        }
        // Send the complete bounded prefix before waiting so Kafka can batch and pipeline it.
        for (var item : pending) {
          long remaining = publicationDeadlineNanos - (System.nanoTime() - publishStarted);
          if (remaining <= 0) throw new TimeoutException("Batch publication deadline exceeded");
          var position = item.publication().get(remaining, TimeUnit.NANOSECONDS);
          confirmed.add(new KafkaAcceptance.Confirmed(item.entry().sequence(), position));
        }
      } finally {
        publication.record(System.nanoTime() - publishStarted, TimeUnit.NANOSECONDS);
      }
      long checkpointStarted = System.nanoTime();
      try {
        acceptance.confirmBatch(confirmed);
      } finally {
        checkpoint.record(System.nanoTime() - checkpointStarted, TimeUnit.NANOSECONDS);
      }
      delivered.increment(entries.size());
      success = true;
      return entries.size();
    } catch (InterruptedException stopped) {
      throw stopped;
    } catch (Exception unconfirmed) {
      return -1;
    } finally {
      for (var span : spans) {
        if (!success) span.setStatus(StatusCode.ERROR, "Delivery unconfirmed");
        telemetry.completed(PlatformTelemetry.Stage.DISPATCH, started, success);
        span.end();
      }
    }
  }

  public String fatalFailure() {
    return fatal.get();
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public int getPhase() {
    return 1000;
  }

  @Override
  public synchronized void stop() {
    running.set(false);
    Thread current = worker;
    if (current == null) return;
    current.interrupt();
    try {
      current.join(12000);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
    if (current.isAlive())
      throw new IllegalStateException("Dispatcher failed to stop within its deadline");
  }

  @Override
  public void stop(Runnable callback) {
    try {
      stop();
    } finally {
      callback.run();
    }
  }
}
