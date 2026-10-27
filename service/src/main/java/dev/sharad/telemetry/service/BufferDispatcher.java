package dev.sharad.telemetry.service;

import dev.sharad.telemetry.buffer.DiskBuffer;
import dev.sharad.telemetry.core.DurableLog;
import dev.sharad.telemetry.observability.PlatformTelemetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;

/** One ordered dispatcher per exclusive volume; failure leaves the head available for replay. */
public final class BufferDispatcher implements SmartLifecycle {
  private final DiskBuffer buffer;
  private final KafkaAcceptance acceptance;
  private final KafkaTemplate<String, byte[]> kafka;
  private final PlatformTelemetry telemetry;
  private final String topic;
  private final Counter delivered;
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
    this.buffer = buffer;
    this.acceptance = acceptance;
    this.kafka = kafka;
    this.telemetry = telemetry;
    this.topic = topic;
    delivered = Counter.builder("telemetry.buffer.delivered").register(meters);
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
    while (running.get()) {
      DiskBuffer.Entry entry;
      try {
        var next = buffer.peek();
        if (next.isEmpty()) {
          Thread.sleep(50);
          continue;
        }
        entry = next.orElseThrow();
      } catch (InterruptedException stopped) {
        Thread.currentThread().interrupt();
        break;
      } catch (IOException corrupt) {
        fatal.set("buffer-read-failure");
        running.set(false);
        break;
      }
      long started = System.nanoTime();
      boolean success = false;
      var span =
          telemetry.start(
              PlatformTelemetry.Stage.DISPATCH, SpanKind.PRODUCER, entry.traceHeaders());
      try {
        var record = new ProducerRecord<String, byte[]>(topic, entry.key(), entry.payload());
        telemetry
            .inject(span)
            .forEach(
                (key, value) -> record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
        var metadata = kafka.send(record).get(10, TimeUnit.SECONDS).getRecordMetadata();
        // A timeout never advances the checkpoint. The retry may append a duplicate, which the sink
        // handles.
        acceptance.confirm(
            entry.sequence(),
            new DurableLog.Position(metadata.topic(), metadata.partition(), metadata.offset()));
        delivered.increment();
        success = true;
        failures = 0;
      } catch (InterruptedException stopped) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception unconfirmed) {
        span.setStatus(StatusCode.ERROR, "Delivery unconfirmed");
        failures++;
      } finally {
        telemetry.completed(PlatformTelemetry.Stage.DISPATCH, started, success);
        span.end();
      }
      if (!success) {
        try {
          Thread.sleep(Math.min(5000L, 200L << Math.min(failures, 4)));
        } catch (InterruptedException stopped) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    running.set(false);
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
