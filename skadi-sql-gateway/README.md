# skadi-sql-gateway

PostgreSQL wire-protocol gateway that lets Tableau, DBeaver, and any `psql` client connect
to Databricks SQL Warehouse as if it were a PostgreSQL server.

---

## Quick start

```bash
# From repo root — run locally
./mvnw spring-boot:run -pl skadi-sql-gateway

# Connect with psql (trust mode by default)
psql -h 127.0.0.1 -p 15432 -U demo postgres -c "SELECT 1"
```

---

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| `15432` | TCP — PostgreSQL wire | Tableau, DBeaver, psql |
| `8090` | HTTP | Spring Boot Actuator (health, metrics, Prometheus) |

---

## Auth modes

Configured via `skadi.sql-gateway.pgwire.auth.mode`:

| Mode | Description |
|---|---|
| `trust` (default) | Accepts any username, no password required. Dev/CI only. |
| `password` | Validates credentials against configured users. Use for production. |

Example `application.yml` for password mode:

```yaml
skadi:
  sql-gateway:
    pgwire:
      auth:
        mode: password
        credential-store: bcrypt   # plaintext | bcrypt
        users:
          alice: "$2a$12$..."      # bcrypt hash
        policies:
          alice:
            allowed-schemas: [sales]
```

---

## Build and test

```bash
# All tests (from repo root)
./mvnw verify

# This module only
./mvnw test -pl skadi-sql-gateway

# Build JAR (skip tests)
./mvnw package -DskipTests -pl skadi-sql-gateway -am
```

Tests use real objects — no Mockito mocks. See `src/test/java` for the test suite.

---

## Docker

```bash
# Build image
docker build -f skadi-sql-gateway/Dockerfile -t skadi-sql-gateway:latest .

# Run with docker compose (from skadi-sql-gateway/)
cp .env.example .env   # fill in Databricks credentials
docker compose up -d

# Verify
curl -s http://localhost:8090/actuator/health | python3 -m json.tool
psql -h localhost -p 15432 -U demo postgres -c "SELECT 1"
```

Full Docker deployment guide: [docs/deployment/docker.md](../docs/deployment/docker.md)

---

## Configuration reference

All settings are under `skadi.sql-gateway.*` in `application.yml`, or as environment
variables (Spring Boot convention: replace `.` and `-` with `_`, uppercase).

| Key | Env var | Default | Notes |
|---|---|---|---|
| `pgwire.enabled` | `SKADI_SQL_GATEWAY_PGWIRE_ENABLED` | `false` | Must be `true` |
| `pgwire.port` | `SKADI_SQL_GATEWAY_PGWIRE_PORT` | `15432` | |
| `pgwire.auth.mode` | `SKADI_SQL_GATEWAY_PGWIRE_AUTH_MODE` | `trust` | Use `password` for production |
| `pgwire.query-timeout` | `SKADI_SQL_GATEWAY_PGWIRE_QUERY_TIMEOUT` | *(none)* | e.g. `5m` |
| `databricks.enabled` | `SKADI_SQL_GATEWAY_DATABRICKS_ENABLED` | `false` | Must be `true` |
| `databricks.host` | `SKADI_SQL_GATEWAY_DATABRICKS_HOST` | | Workspace hostname |
| `databricks.http-path` | `SKADI_SQL_GATEWAY_DATABRICKS_HTTP_PATH` | | Warehouse HTTP path |
| `databricks.token` | `SKADI_SQL_GATEWAY_DATABRICKS_TOKEN` | | PAT token (keep secret) |
| `metadata.dbx-catalog` | `SKADI_SQL_GATEWAY_METADATA_DBX_CATALOG` | `main` | Unity Catalog name |
| `metadata.dbx-schema` | `SKADI_SQL_GATEWAY_METADATA_DBX_SCHEMA` | `public` | Schema to expose |
| `cache.enabled` | `SKADI_SQL_GATEWAY_CACHE_ENABLED` | `true` | |
| `cache.ttl` | `SKADI_SQL_GATEWAY_CACHE_TTL` | `5m` | |

---

## Health endpoints

| Endpoint | Purpose |
|---|---|
| `GET /actuator/health` | Overall health |
| `GET /actuator/health/pgWire` | pgwire listener readiness (use as k8s readiness probe) |
| `GET /actuator/prometheus` | Prometheus metrics scrape |
| `GET /ping` | Simple liveness (`ok`) |

---

## Deployment guides

- [Docker deployment](../docs/deployment/docker.md)
- [Tableau Server](../docs/deployment/tableau-server.md)
- [Tableau Bridge / Cloud](../docs/deployment/tableau-bridge.md)
- [Production checklist](../docs/deployment/production-checklist.md)
- [Troubleshooting](../docs/deployment/troubleshooting.md)
