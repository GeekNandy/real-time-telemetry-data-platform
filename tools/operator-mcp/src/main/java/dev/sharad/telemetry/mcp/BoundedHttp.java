package dev.sharad.telemetry.mcp;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

/** Fixed-size response and absolute whole-body deadline, including slow response bodies. */
final class BoundedHttp implements AutoCloseable {
  static final int BODY_LIMIT = 32 * 1024;
  private final HttpClient client =
      HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(1))
          .followRedirects(HttpClient.Redirect.NEVER)
          .build();
  private final Duration deadline;

  BoundedHttp() {
    this(Duration.ofSeconds(3));
  }

  BoundedHttp(Duration deadline) {
    this.deadline = deadline;
  }

  byte[] get(URI origin, String path, String token) throws Failure {
    var body = new LimitedBody();
    CompletableFuture<HttpResponse<byte[]>> response = null;
    try {
      var request =
          HttpRequest.newBuilder(origin.resolve(path))
              .timeout(deadline)
              .header("Accept", "application/json")
              .header("Accept-Encoding", "identity")
              .GET();
      if (token != null) {
        request.header("Authorization", "Bearer " + token);
      }
      response = client.sendAsync(request.build(), ignored -> body);
      var result = response.get(deadline.toMillis(), TimeUnit.MILLISECONDS);
      if (result.statusCode() != 200) {
        throw new Failure(
            switch (result.statusCode()) {
              case 401 -> "AUTHENTICATION_REQUIRED";
              case 403 -> "OBSERVE_PERMISSION_REQUIRED";
              case 301, 302, 303, 307, 308 -> "REDIRECT_REFUSED";
              default -> "UPSTREAM_UNAVAILABLE";
            });
      }
      return result.body();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new Failure("CANCELLED");
    } catch (Failure error) {
      throw error;
    } catch (Exception error) {
      throw new Failure("UPSTREAM_UNAVAILABLE");
    } finally {
      body.cancel();
      if (response != null && !response.isDone()) {
        response.cancel(true);
      }
    }
  }

  @Override
  public void close() {
    client.shutdownNow();
  }

  static final class Failure extends Exception {
    private static final long serialVersionUID = 1L;

    Failure(String code) {
      super(code);
    }
  }

  private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final CompletableFuture<byte[]> result = new CompletableFuture<>();
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private Flow.Subscription subscription;
    private boolean cancelled;

    @Override
    public CompletionStage<byte[]> getBody() {
      return result;
    }

    @Override
    public synchronized void onSubscribe(Flow.Subscription next) {
      if (cancelled || subscription != null) {
        next.cancel();
      } else {
        subscription = next;
        next.request(1);
      }
    }

    @Override
    public synchronized void onNext(List<ByteBuffer> buffers) {
      if (cancelled) {
        return;
      }
      for (ByteBuffer buffer : buffers) {
        if (buffer.remaining() > BODY_LIMIT - bytes.size()) {
          cancel();
          result.completeExceptionally(new Failure("UPSTREAM_RESPONSE_TOO_LARGE"));
          return;
        }
        byte[] part = new byte[buffer.remaining()];
        buffer.get(part);
        bytes.writeBytes(part);
      }
      subscription.request(1);
    }

    @Override
    public void onError(Throwable error) {
      result.completeExceptionally(new Failure("UPSTREAM_UNAVAILABLE"));
    }

    @Override
    public synchronized void onComplete() {
      result.complete(bytes.toByteArray());
    }

    synchronized void cancel() {
      cancelled = true;
      if (subscription != null) {
        subscription.cancel();
      }
    }
  }
}
