package dev.sharad.telemetry.core;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Component overhead experiment with an immediate-ack test double. Excludes HTTP, JWT, JSON, Kafka,
 * PostgreSQL and fingerprinting. Never an end-to-end throughput claim.
 */
public final class CoreBenchmark {
  private CoreBenchmark() {}

  public static void main(String[] arguments) throws Exception {
    int clients = arguments.length > 0 ? Integer.parseInt(arguments[0]) : 8;
    int perClient = arguments.length > 1 ? Integer.parseInt(arguments[1]) : 50_000;
    if (clients < 1 || clients > 64 || perClient < 1000 || perClient > 1_000_000) {
      throw new IllegalArgumentException(
          "Require 1..64 clients and 1000..1000000 samples per client");
    }
    var event =
        new Event(
            new UUID(0, 1),
            "component-test",
            "session.started",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            Map.of("platform", "linux"));
    var sequence = new AtomicLong();
    try (var engine =
        new IngestionEngine(
            ignored ->
                CompletableFuture.completedFuture(
                    new DurableLog.Position(
                        "synthetic-immediate-ack", 0, sequence.getAndIncrement())),
            256,
            64,
            4)) {
      run(engine, event, clients, 10_000); // Warmup excluded from the reported samples.
      long before = System.nanoTime();
      long[][] samples = run(engine, event, clients, perClient);
      long elapsed = System.nanoTime() - before;
      long[] all = Arrays.stream(samples).flatMapToLong(Arrays::stream).toArray();
      Arrays.sort(all);
      var snapshot = engine.snapshot();
      long expected = (long) clients * (perClient + 10_000);
      if (snapshot.accepted() != expected
          || snapshot.failed() != 0
          || snapshot.rejected() != 0
          || snapshot.admission().inFlight() != 0) {
        throw new AssertionError("Experiment did not reconcile completion counts: " + snapshot);
      }
      String report =
          String.format(
              Locale.ROOT,
              """
              {
                "experiment": "core-immediate-ack-overhead",
                "recordedAt": "%s",
                "sourceRevision": "%s",
                "sourceDirty": %s,
                "javaVersion": "%s",
                "vm": "%s",
                "os": "%s",
                "architecture": "%s",
                "availableProcessors": %d,
                "jvmArguments": "%s",
                "clients": %d,
                "sendThreads": 4,
                "globalCapacity": 256,
                "tenantCapacity": 64,
                "warmupPerClient": 10000,
                "samples": %d,
                "elapsedSeconds": %.6f,
                "completionsPerSecond": %.3f,
                "latencyMicroseconds": {"p50": %.3f, "p95": %.3f, "p99": %.3f, "max": %.3f},
                "failed": 0,
                "rejected": 0,
                "broker": "immediate completed future test double",
                "excludes": ["HTTP", "JWT", "JSON", "Kafka", "PostgreSQL", "fingerprinting"],
                "loadModel": "closed loop; one outstanding operation per client; coordinated omission applies"
              }
              """,
              Instant.now(),
              escape(System.getProperty("benchmark.revision", "unknown")),
              Boolean.parseBoolean(System.getProperty("benchmark.dirty", "true")),
              escape(System.getProperty("java.version")),
              escape(System.getProperty("java.vm.name")),
              escape(System.getProperty("os.name")),
              escape(System.getProperty("os.arch")),
              Runtime.getRuntime().availableProcessors(),
              escape(String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments())),
              clients,
              all.length,
              elapsed / 1e9,
              all.length / (elapsed / 1e9),
              percentile(all, 0.50) / 1000.0,
              percentile(all, 0.95) / 1000.0,
              percentile(all, 0.99) / 1000.0,
              all[all.length - 1] / 1000.0);
      String destination = System.getProperty("benchmark.output");
      if (destination == null) System.out.print(report);
      else Files.writeString(Path.of(destination), report);
    }
  }

  private static long[][] run(IngestionEngine engine, Event event, int clients, int count)
      throws Exception {
    var pool = Executors.newFixedThreadPool(clients);
    var start = new CountDownLatch(1);
    try {
      List<java.util.concurrent.Future<long[]>> futures = new ArrayList<>();
      for (int index = 0; index < clients; index++) {
        futures.add(
            pool.submit(
                () -> {
                  long[] samples = new long[count];
                  start.await();
                  for (int sample = 0; sample < count; sample++) {
                    long before = System.nanoTime();
                    engine.accept(event).toCompletableFuture().get(5, TimeUnit.SECONDS);
                    samples[sample] = System.nanoTime() - before;
                  }
                  return samples;
                }));
      }
      start.countDown();
      long[][] result = new long[clients][];
      for (int index = 0; index < clients; index++)
        result[index] = futures.get(index).get(90, TimeUnit.SECONDS);
      return result;
    } finally {
      pool.shutdownNow();
      if (!pool.awaitTermination(5, TimeUnit.SECONDS))
        throw new IllegalStateException("Client workers did not stop");
    }
  }

  private static long percentile(long[] sorted, double fraction) {
    return sorted[Math.max(0, (int) Math.ceil(sorted.length * fraction) - 1)];
  }

  private static String escape(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
