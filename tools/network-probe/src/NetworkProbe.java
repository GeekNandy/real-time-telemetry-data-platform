import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Synthetic peer: an HTTP rejection is not network isolation. */
public final class NetworkProbe {
  public static void main(String[] args) throws Exception {
    if (args.length != 3) throw new IllegalArgumentException("mode target expectation");
    if (args[0].equals("serve")) {
      try (var server = new ServerSocket(Integer.parseInt(args[1]))) {
        while (!Thread.currentThread().isInterrupted()) try (var accepted = server.accept()) {
          accepted.setSoTimeout(1000);
        }
      }
      return;
    }
    boolean connected = false;
    try {
      if (args[0].equals("http")) {
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
          var response = client.send(HttpRequest.newBuilder(URI.create(args[1]))
              .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.discarding());
          connected = true;
          if (args[2].equals("allow") && response.statusCode() != 200)
            throw new IllegalStateException("Allowed path must be healthy");
        }
      } else if (args[0].equals("tcp")) {
        String[] target = args[1].split(":");
        try (var socket = new Socket()) {
          socket.connect(new InetSocketAddress(target[0], Integer.parseInt(target[1])), 2000);
          connected = true;
        }
      } else throw new IllegalArgumentException("Unknown probe mode");
    } catch (IOException denied) {
      if (args[2].equals("allow")) throw denied;
    }
    if (!args[2].equals("allow") && !args[2].equals("deny")) throw new IllegalArgumentException("Expectation");
    if (connected != args[2].equals("allow")) throw new IllegalStateException("Network policy expectation failed");
    System.out.println("PASS " + args[0] + " " + args[2]);
  }
}
