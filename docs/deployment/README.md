# Skadi SQL Gateway — Deployment Guide

Skadi SQL Gateway exposes a PostgreSQL wire-protocol endpoint that Tableau Desktop,
Tableau Server, Tableau Bridge, DBeaver, and any PostgreSQL JDBC client can connect to,
proxying queries through to Databricks SQL Warehouse.

---

## Quick navigation

| Guide | When to use |
|---|---|
| [Docker deployment](docker.md) | Containerised deployment (recommended for production) |
| [Tableau Server](tableau-server.md) | Connecting Tableau Server to the gateway |
| [Tableau Bridge / Cloud](tableau-bridge.md) | Connecting Tableau Cloud via Tableau Bridge |
| [Production checklist](production-checklist.md) | Pre-go-live verification |
| [Troubleshooting](troubleshooting.md) | Common issues and fixes |

---

## Architecture overview

```
Tableau Desktop / Server / Bridge
        │  PostgreSQL wire protocol (port 15432)
        ▼
  Skadi SQL Gateway
  ┌─────────────────────────────────────────────────────┐
  │  pgwire listener          (PgWireServer)            │
  │  SQL dialect bridge       (SqlDialectBridge)        │
  │  Query result cache       (QueryResultCache)        │
  │  Metadata facade          (MetadataQueryRouter)     │
  └──────────────────────────┬──────────────────────────┘
                             │  Databricks JDBC
                             ▼
                  Databricks SQL Warehouse
```

---

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| `15432` | TCP — PostgreSQL wire | Tableau, DBeaver, psql client connections |
| `8090` | HTTP | Spring Boot Actuator (health, metrics, Prometheus) |

---

## Minimum requirements

| Component | Requirement |
|---|---|
| Java | 21+ (Eclipse Temurin recommended) |
| RAM | 512 MB minimum; 2 GB recommended (cache + JDBC pool) |
| Network | Outbound TCP to Databricks workspace on port 443 |
| Databricks | SQL Warehouse (Serverless or Pro); HTTP path + PAT token |
| Tableau | Desktop 2022.1+, Server 2022.1+, or Bridge latest |

---

## Key configuration reference

All settings can be supplied as **environment variables** using Spring Boot's convention:
replace `.` with `_` and `-` with `_`, then uppercase.

| YAML key | Environment variable | Default | Notes |
|---|---|---|---|
| `skadi.sql-gateway.pgwire.enabled` | `SKADI_SQL_GATEWAY_PGWIRE_ENABLED` | `false` | Must be `true` |
| `skadi.sql-gateway.pgwire.port` | `SKADI_SQL_GATEWAY_PGWIRE_PORT` | `15432` | |
| `skadi.sql-gateway.pgwire.auth.mode` | `SKADI_SQL_GATEWAY_PGWIRE_AUTH_MODE` | `trust` | Use `password` for production |
| `skadi.sql-gateway.pgwire.query-timeout` | `SKADI_SQL_GATEWAY_PGWIRE_QUERY_TIMEOUT` | *(none)* | e.g. `5m` |
| `skadi.sql-gateway.pgwire.max-concurrent-queries-per-user` | `SKADI_SQL_GATEWAY_PGWIRE_MAX_CONCURRENT_QUERIES_PER_USER` | `0` (unlimited) | |
| `skadi.sql-gateway.databricks.enabled` | `SKADI_SQL_GATEWAY_DATABRICKS_ENABLED` | `false` | Must be `true` |
| `skadi.sql-gateway.databricks.host` | `SKADI_SQL_GATEWAY_DATABRICKS_HOST` | | `<workspace>.azuredatabricks.net` |
| `skadi.sql-gateway.databricks.http-path` | `SKADI_SQL_GATEWAY_DATABRICKS_HTTP_PATH` | | `/sql/1.0/warehouses/<id>` |
| `skadi.sql-gateway.databricks.token` | `SKADI_SQL_GATEWAY_DATABRICKS_TOKEN` | | PAT token (keep secret) |
| `skadi.sql-gateway.databricks.max-pool-size` | `SKADI_SQL_GATEWAY_DATABRICKS_MAX_POOL_SIZE` | `5` | |
| `skadi.sql-gateway.metadata.dbx-catalog` | `SKADI_SQL_GATEWAY_METADATA_DBX_CATALOG` | `main` | Unity Catalog name |
| `skadi.sql-gateway.metadata.dbx-schema` | `SKADI_SQL_GATEWAY_METADATA_DBX_SCHEMA` | `public` | Schema to expose |
| `skadi.sql-gateway.cache.enabled` | `SKADI_SQL_GATEWAY_CACHE_ENABLED` | `true` | |
| `skadi.sql-gateway.cache.ttl` | `SKADI_SQL_GATEWAY_CACHE_TTL` | `5m` | |

> **Secret handling:** never pass `SKADI_SQL_GATEWAY_DATABRICKS_TOKEN` as a plain environment
> variable in a shared environment. Use your orchestrator's secret management (k8s Secrets,
> Docker secrets, Vault, AWS Secrets Manager) to inject it at runtime.

---

## Health checks

| Endpoint | Purpose |
|---|---|
| `GET /actuator/health` | Overall application health (includes pgwire listener status) |
| `GET /actuator/health/pgWire` | pgwire listener specifically (use as readiness probe) |
| `GET /actuator/metrics` | Micrometer metric names |
| `GET /actuator/prometheus` | Prometheus scrape endpoint |
| `GET /ping` | Simple liveness check (returns `ok`) |

The `pgWire` health indicator reports `UP` only once the pgwire socket is bound and accepting
connections. Use it as the **readiness probe** in k8s or load-balancer health checks so traffic
only reaches the gateway after the pgwire port is ready.

---

## Next steps

- [Docker deployment guide](docker.md) — getting the gateway running in 5 minutes
- [Tableau Server guide](tableau-server.md) — setting up published data sources
- [Tableau Bridge / Cloud guide](tableau-bridge.md) — connecting Tableau Cloud
