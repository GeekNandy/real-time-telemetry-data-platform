package dev.sharad.telemetry.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Accumulates at most the configured response budget and cancels oversized publishers. */
final class BoundedRegistryBody implements HttpResponse.BodySubscriber<byte[]> {
  private final int maximumBytes;
  private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private final CompletableFuture<byte[]> body = new CompletableFuture<>();
  private volatile Flow.Subscription subscription;
  private int received;

  BoundedRegistryBody(int maximumBytes) {
    if (maximumBytes < 1) throw new IllegalArgumentException("Response budget must be positive");
    this.maximumBytes = maximumBytes;
  }

  @Override
  public CompletionStage<byte[]> getBody() {
    return body;
  }

  @Override
  public void onSubscribe(Flow.Subscription candidate) {
    if (subscription != null) {
      candidate.cancel();
      return;
    }
    subscription = candidate;
    if (body.isDone()) candidate.cancel();
    else candidate.request(1);
  }

  @Override
  public void onNext(List<ByteBuffer> buffers) {
    if (body.isDone()) return;
    for (ByteBuffer buffer : buffers) {
      int size = buffer.remaining();
      if (size > maximumBytes - received) {
        body.completeExceptionally(new IOException("Registry response exceeds limit"));
        subscription.cancel();
        return;
      }
      byte[] chunk = new byte[size];
      buffer.get(chunk);
      bytes.writeBytes(chunk);
      received += size;
    }
    subscription.request(1);
  }

  @Override
  public void onError(Throwable failure) {
    body.completeExceptionally(failure);
  }

  @Override
  public void onComplete() {
    body.complete(bytes.toByteArray());
  }

  void cancel() {
    body.completeExceptionally(new IOException("Registry response cancelled"));
    Flow.Subscription current = subscription;
    if (current != null) current.cancel();
  }
}
