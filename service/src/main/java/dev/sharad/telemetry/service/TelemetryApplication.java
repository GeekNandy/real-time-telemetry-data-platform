package dev.sharad.telemetry.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@org.springframework.scheduling.annotation.EnableScheduling
@SpringBootApplication
public class TelemetryApplication {
  public static void main(String[] args) {
    SpringApplication.run(TelemetryApplication.class, args);
  }
}
