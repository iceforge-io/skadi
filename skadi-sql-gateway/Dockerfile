# ─── Build stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace

# Copy Maven wrapper and parent POM (cache layer for dependency downloads)
COPY mvnw ./
COPY .mvn .mvn/
COPY pom.xml ./
COPY skadi-core/pom.xml skadi-core/
COPY skadi-server/pom.xml skadi-server/
COPY skadi-sql-gateway/pom.xml skadi-sql-gateway/

RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw dependency:go-offline -pl skadi-sql-gateway -am -q --no-transfer-progress 2>/dev/null || true

COPY skadi-core/src skadi-core/src/
COPY skadi-server/src skadi-server/src/
COPY skadi-sql-gateway/src skadi-sql-gateway/src/

RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw package -pl skadi-sql-gateway -am -DskipTests --no-transfer-progress

# ─── Runtime stage ────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy

RUN groupadd --system skadi && useradd --system --gid skadi --no-create-home skadi
WORKDIR /app

COPY --from=build /workspace/skadi-sql-gateway/target/skadi-sql-gateway-*.jar app.jar

# NOTE: The Databricks JDBC driver is NOT included (provided/optional scope).
# To add it, extend this image:
#   FROM ghcr.io/iceforge-io/skadi-sql-gateway:latest
#   COPY databricks-jdbc-2.x.jar /app/BOOT-INF/lib/
# Or mount it and set JAVA_TOOL_OPTIONS=-cp /opt/drivers/databricks-jdbc.jar

USER skadi

# pgwire — Tableau / DBeaver / psql connect here
EXPOSE 15432
# HTTP — Spring Boot Actuator (health, metrics, Prometheus)
EXPOSE 8090

HEALTHCHECK --interval=15s --timeout=5s --start-period=45s --retries=3 \
    CMD wget -q -O /dev/null "http://localhost:8090/actuator/health" || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
