FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY core core
COPY service service
RUN mvn -B -ntp verify

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/service/target/telemetry-service-0.1.0-SNAPSHOT.jar /app/app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
