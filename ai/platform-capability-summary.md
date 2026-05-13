# Skadi — Platform Capability Summary

> As of: 2026-05-13 (post-EPIC #10, Lanes A + B complete)
> For use in: ADR context sections, architecture proposals, stakeholder briefs

---

## What Skadi Is

A production-hardened SQL gateway that accepts PostgreSQL and MySQL wire-protocol connections
from BI tools and routes them to Databricks SQL Warehouse, with in-memory result caching,
dialect translation, and structured observability.

---

## Wire Protocol

| Protocol | Status | Port (default) | Auth modes |
|---|---|---|---|
| PostgreSQL (pgwire) | Production-ready | 15432 | trust, plaintext, bcrypt |
| MySQL wire | Production-ready | 13306 | trust, `mysql_native_password` |

Both protocols share the same execution core (dialect bridge, JDBC executor, cache, audit log).

Supported pgwire messages: `SSLRequest`, `StartupMessage`, `PasswordMessage`, `AuthenticationOk`,
`ParameterStatus`, `BackendKeyData`, `ReadyForQuery`, `Query (Q)`, `Parse (P)`, `Bind (B)`,
`Describe (D)`, `Execute (E)`, `Close (C)`, `Sync (H/S)`, `Terminate (X)`, `CancelRequest`.

---

## SQL Execution

| Capability | Detail |
|---|---|
| Dialect translation | PG `"ident"` → Databricks `` `ident` ``; `expr::type` → `CAST`; `$n` → `?`; MySQL `LIMIT/OFFSET` normalisation |
| SQL normalisation | Deterministic whitespace/keyword/comment collapse for cache-key generation |
| Parameter binding | Extended-query `Bind (B)` params parsed, text/binary format codes honoured, forwarded to `PreparedStatement` |
| Execution backend | Databricks JDBC connection pool (`DatabricksJdbcExecutor`); configurable pool size, timeout, `fetchSize`, `maxRows` |
| Metadata facade | Synthetic responses for `information_schema`, `pg_catalog`, `SHOW`, `SET`, `current_database()`, `version()`, `SELECT 1` — never touches Databricks for metadata |

---

## Authentication & Authorisation

| Feature | Detail |
|---|---|
| Auth modes | `trust` (any user), `password` + plaintext store, `password` + bcrypt store |
| Per-user schema ACL | `PrincipalPolicyRegistry` — `allowed-schemas` list per principal; enforced at metadata query level |
| Session isolation | One thread per session; session-scoped MDC context (`session_id`, `client`) |

---

## Resource Controls

| Feature | Config key | Default |
|---|---|---|
| Per-user concurrency cap | `pgwire.max-concurrent-queries-per-user` | unlimited |
| Query timeout | `pgwire.query-timeout` | none |
| Cancellation | Protocol `CancelRequest` → `Statement.cancel()` + Arrow cancel flag | always on |
| SQL injection prevention | `SqlSecurityValidator` — rejects queries >1 MiB or containing null bytes | always on |

---

## Caching

| Layer | Format | Key | TTL |
|---|---|---|---|
| Path 3 (preferred) | Arrow IPC bytes | `SHA-256(user, normalized_sql, params)` | `CacheProperties.queryResultTtl` (default 2m) |
| Path 1 (fallback) | Text rows | `SHA-256(user, normalized_sql)` — no params | `SqlGatewayProperties.Cache.ttl` (default 5m) |
| Metadata cache | Text rows | Query pattern match | 2m |

**Known gap:** dataset version is absent from all cache keys (debt L3). Stale results are
possible if upstream data refreshes within the TTL window.

---

## Observability

| Signal | Detail |
|---|---|
| Metrics | Micrometer/Prometheus at `/actuator/prometheus`; `skadi_sessions_active`, `skadi_queries_seconds` (p50/p95/p99), `skadi_query_errors_total` |
| Cache tier tags | `hit`, `miss`, `skip`, `arrow_hit`, `arrow_miss` on `skadi_queries_seconds` |
| Structured logs | SLF4J + MDC; fields: `session_id`, `query_id`, `client` |
| Correlation IDs | `query_id` forwarded to Databricks via `ApplicationName` for cross-system tracing |
| Audit log | `skadi.audit` logger; `audit_connect`, `audit_query`, `audit_error`; never includes SQL text or parameter values |
| Trace corpus | Optional `.jsonl` per-session corpus writer under `testdata/tableau-traces/` |

---

## Security

| Feature | Status |
|---|---|
| Secret redaction | `SecretRedactor` strips sensitive keys from startup params before logging |
| Token masking | `DatabricksProperties.toString()` replaces token with `***` |
| Audit log | Always-on; log-injection protected (newline sanitisation); `additivity=false` for SIEM routing |
| TLS config | `SqlGatewayProperties.PgWire.Tls` record; `require-ssl=true` enforcement tested |
| STARTTLS upgrade | **Not implemented** (debt L22) — use stunnel/Envoy proxy for production TLS |

---

## Deployment

| Artifact | Location |
|---|---|
| Docker image | `skadi-sql-gateway/Dockerfile` (multi-stage, non-root `skadi` user) |
| Docker Compose | `skadi-sql-gateway/docker-compose.yml` (full env-var mapping) |
| k8s manifest | `docs/deployment/docker.md` (readiness probe on `/actuator/health/pgWire`) |
| Health endpoints | `/actuator/health/pgWire`, `/actuator/health/mySqlWire`, `/actuator/health` |
| Smoke test | `scripts/smoke-test.sh` (exit 0/1; suitable for CI gate) |
| Deployment guides | `docs/deployment/` — Docker, Tableau Server, Tableau Bridge, production checklist, troubleshooting |

---

## Test Coverage

| Module | Tests | Harness |
|---|---|---|
| `skadi-sql-gateway` | 232 | H2-backed `GatewayCorrectnessHarness`; no Databricks connection required |
| `skadi-core` | included in parent build | `JdbcArrowStreamer` unit + prepared-statement tests |

All tests pass without Databricks credentials. Real integration tests (`SqlGatewayIT`) are
placeholder-only pending CI secrets (debt L16).

---

## Module Structure (Actual vs Intended)

| Module | Intended role | Actual role | Gap |
|---|---|---|---|
| `skadi-core` | Shared utilities (normalisation, hashing, Arrow) | Contains only `JdbcArrowStreamer` | Normalisation and hashing logic lives in `skadi-sql-gateway` instead |
| `skadi-server` | Execution engine + cache; called by gateway | Standalone HTTP Arrow API + S3 cache; **never called by gateway** | Gateway→server seam undefined at runtime |
| `skadi-sql-gateway` | Thin protocol adapter; delegates to `skadi-server` | Self-contained service with own JDBC pool, cache, executor | L10: gateway is not thin |

The gateway→server seam is the primary structural debt. Lane C resolves it by making
`skadi-semantic` the first real caller of `skadi-server`'s HTTP API.

---

## What Skadi Is NOT (Boundaries)

- Not a query optimiser or SQL execution engine (Databricks executes all SQL)
- Not a full PostgreSQL server (implements only the BI-tool-required protocol subset)
- Not a data transformation layer (no ETL, no column computation beyond what Databricks returns)
- Not a multi-tenant SaaS platform (single-deployment model; per-user ACL is schema-level only)
- Not a Tableau replacement (Tableau remains the primary BI tool; Skadi is the accelerating proxy)

---

## Reuse Map for Lane C (Semantic Layer)

| Existing component | Reuse |
|---|---|
| `SqlNormalizer` | Normalise compiled semantic SQL before passing to `skadi-server` |
| `SqlDialectBridge` | Compile semantic query output to Databricks SQL |
| `DatabricksJdbcExecutor` | Used inside `skadi-server`; semantic layer delegates there |
| `QueryResultCache` + Arrow path | `skadi-server` is the cache owner for semantic queries |
| `PrincipalPolicyRegistry` pattern | Extended to dataset/metric-level access in `skadi-semantic` |
| `AuditLog` pattern | Extended with `audit_semantic_query` and `source=ai_chat` events |
| `GatewayMetrics` / Micrometer pattern | `skadi-semantic` adds its own meters to the same Prometheus endpoint |
| Dockerfile / docker-compose pattern | `skadi-semantic` follows same packaging model |
