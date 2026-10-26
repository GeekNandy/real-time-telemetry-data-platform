package dev.sharad.telemetry.service;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!collector")
public class DatabaseConfiguration {
  @Bean(destroyMethod = "dispose")
  reactor.core.scheduler.Scheduler jdbcScheduler() {
    return reactor.core.scheduler.Schedulers.newBoundedElastic(8, 128, "telemetry-jdbc", 60, true);
  }

  @Bean
  Flyway flyway(DataSource dataSource) {
    Flyway flyway =
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load();
    flyway.migrate();
    return flyway;
  }
}
