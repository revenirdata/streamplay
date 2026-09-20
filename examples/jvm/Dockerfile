FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -ntp dependency:go-offline
COPY src src
RUN mvn -B -ntp package
FROM eclipse-temurin:17.0.12_7-jre
WORKDIR /app
COPY --from=build /build/target/streamplay-examples-1.0.0.jar /app/examples.jar
ENTRYPOINT ["java", "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED", "-Xmx768m", "-cp", "/app/examples.jar"]
