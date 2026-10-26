# Collector operations

Requires JDK 21, Kafka and a Confluent-compatible Schema Registry. The Java
integration suite supplies isolated real broker/registry processes and exercises
the packaged HTTP service without needing a separately installed cluster.

```bash
./mvnw --batch-mode --no-transfer-progress -f tools/schema-registry-runtime/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath -Dmdep.outputFile=target/classpath.txt
./mvnw --batch-mode --no-transfer-progress -Pregistry-integration verify
```

The distribution is a runnable service JAR plus service/target/lib; keep them
together. The default role in this collector-only revision is collector. Set
SPRING_PROFILES_ACTIVE=collector explicitly for an operational deployment.
Configure JWT_ISSUER, JWT_AUDIENCE, KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL
and BUFFER_PATH for the actual environment. Provision schemas before starting
production writers; runtime registration is disabled by default.

POST /v1/events requires telemetry.write. The tenant comes from the verified
tenant_id claim; a client-supplied tenant field is rejected. Prometheus requires
telemetry.observe. Health endpoints expose probe status without private details.

HTTP 202 confirms local-volume durability. A 429 or retryable 503 does not confirm
acceptance; retry ambiguous outcomes using the same event ID and payload. Monitor
buffer bytes, record count, oldest age, append failures and readiness. Stop intake
before altering an owned volume. Do not delete a pending spool to clear an alert.

SIGTERM stops admission and drains real appends within the configured deadline.
Unpublished entries remain on the retained volume for replay. The packaged
collector test also exercises SIGKILL followed by reopening that exact volume.
