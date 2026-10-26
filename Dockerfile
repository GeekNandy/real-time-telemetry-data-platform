FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY core core
COPY platform-config platform-config
COPY durable-buffer durable-buffer
COPY platform-observability platform-observability
COPY event-schemas event-schemas
COPY service service
RUN mvn -B -ntp verify
RUN java -m jdk.jfr/jdk.jfr.internal.tool.Main configure --input profile.jfc --output /build/service/target/profile-safe.jfc 'jdk.InitialEnvironmentVariable#enabled=false' 'jdk.InitialSystemProperty#enabled=false' 'jdk.SystemProcess#enabled=false'

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/service/target/telemetry-service-0.1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build /build/service/target/lib /app/lib
COPY --from=build /build/service/target/profile-safe.jfc /app/profile-safe.jfc
RUN mkdir -p /app/data/wal /profiles && chown -R 10001:10001 /app/data /profiles
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
