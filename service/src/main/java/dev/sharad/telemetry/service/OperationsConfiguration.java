package dev.sharad.telemetry.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Configuration
public class OperationsConfiguration {
  @Bean(destroyMethod = "dispose")
  Scheduler operationsScheduler() {
    // Meter callbacks can wait on a buffer lock held by synchronized filesystem I/O.
    return Schedulers.newBoundedElastic(1, 8, "telemetry-operations", 60, true);
  }
}
