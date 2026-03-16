# Skadi — Current System State

> Updated: 2026-03-15
> Commit: `09ef4a0`
> Reflects actual codebase state, not intended architecture.

---

## 1. Repository Modules and Responsibilities

### skadi-core

**Intended:** shared library — query normalisation, cache key hashing, dataset versioning, Arrow utilities, config models. No Spring Boot, no network code.

**Actual:** contains only `JdbcArrowStreamer`, a utility that streams a JDBC `PreparedStatement` result into an Apache Arrow IPC byte stream. This class is also duplicated independently inside `skadi-server`. Everything else that should live here (normalisation, hashing, versioning) has grown inside the other modules instead.

---

### skadi-server

**Intended:** execution engine — query execution against Databricks, Arrow streaming, cache management (memory/disk/S3), cache invalidation. Called by the gateway.

**Actual:** a fully operational standalone Spring Boot service that is **never called by `skadi-sql-gateway`**. It was built independently and has its own complete feature set:

- HTTP query API (`docs/openapi-skadi-query-v1.yaml`): `POST /api/v1/queries`, `GET /…/status`, `GET /…/results`, `DELETE /…`
- Async query materialisation to S3
- Arrow IPC result streaming (`application/vnd.apache.arrow.stream`)
- S3 cache tier: `AwsSdkS3AccessLayer`, `CachedAwsSdkS3AccessLayer`, `ResultSetToS3ChunkWriter`
- Peer-cache HTTP replication between nodes (`PeerCacheController` / `PeerCacheClient`)
- Pluggable JDBC connection provider SPI
- Cache statistics dashboard and monitoring endpoints
- H2 in-process demo mode

The gateway→server seam is defined (the OpenAPI spec exists) but not implemented.

---

### skadi-sql-gateway

**Intended:** thin protocol adapter — accept pgwire connections, translate SQL, call `skadi-server`, stream Arrow results back as pgwire rows.

**Actual:** a self-contained Spring Boot service that embeds its own JDBC executor and caching. It does not delegate to `skadi-server`. Key components:

| Package | Responsibility |
|---|---|
| `pgwire` | `PgWireServer` (TCP acceptor) + `PgWireSession` (one thread per connection; full message loop) |
| `auth` | `AllowAllAuthProvider`, `PlaintextAuthProvider`, `BcryptAuthProvider`; `PrincipalPolicyRegistry` for per-user schema ACL |
| `dialect` | `SqlDialectBridge` → `PostgresToDatabricksTranslator` / `MySqlToDatabricksTranslator`; `SqlNormalizer`; `ParameterMarkerRewriter` |
| `metadata` | `MetadataQueryRouter` (intercepts `information_schema` / `pg_catalog` queries); `MetadataCache` (TTL) |
| `cache` | `QueryResultCache` (TTL, in-memory, row + Arrow byte storage); `QueryCacheKey`; `QueryResultCacheKey` |
| `executor` | `DatabricksJdbcExecutor` — own connection pool; captures Databricks query IDs |
| `trace` | `TableauTraceLogger` — structured MDC logging; optional `.jsonl` corpus writer |
| `config` | `SqlGatewayProperties` — all config via nested records |
| `api` | Health, ping, info HTTP endpoints |

---

## 2. What Is Currently Implemented

### pgwire protocol

Complete enough for Tableau Desktop and psql. Supported messages:

- Startup sequence: `SSLRequest` → `N`, `StartupMessage`, optional `PasswordMessage`
- Simple Query (`Q`) and minimal Extended Query (`P`/`B`/`D`/`E`/`C`/`H`/`S`)
- `RowDescription`, `DataRow` (text format), `CommandComplete`, `ErrorResponse`, `ReadyForQuery`
- `ParseComplete`, `BindComplete`, `CloseComplete`, `NoData`, `ParameterDescription`
- `Terminate` (`X`)
- **Not implemented:** `CancelRequest`

### Authentication

| Mode | Status |
|---|---|
| `trust` (any user accepted) | ✅ |
| `password` + plaintext credential store | ✅ |
| `password` + bcrypt credential store | ✅ |
| Per-user schema ACL (`PrincipalPolicy`) | ✅ |
| mTLS / OAuth | ❌ |

### Metadata facade

Intercepts and answers synthetically (never touches Databricks for metadata):

- `information_schema.schemata`, `.tables`, `.columns`
- `pg_catalog.pg_namespace`, `.pg_database`
- `current_database()`, `current_schema()`, `version()`
- `SHOW <setting>`, `SET …`, `RESET …`
- `SELECT 1` keepalives

Metadata values (`pgDatabase`, `dbxCatalog`, `dbxSchema`) are hardcoded in `PgWireSession` constructor — not wired from `SqlGatewayProperties.Metadata`.

### SQL dialect translation

- `PG "ident"` → Databricks `` `ident` ``
- PG `expr::type` → `CAST(expr AS type)`
- PG `$n` parameter markers → `?` (with out-of-order rewriting via `ParameterMarkerRewriter`)
- MySQL `LIMIT … OFFSET …` reordering
- SQL normalisation: comment stripping, whitespace collapse, keyword uppercasing, punctuation normalisation

**Caveat:** dialect bridge is only applied on the secondary (conditional) execution path (`PgWireQueryCachingExecutorProvider`). The primary execution path sends raw client SQL to Databricks.

### Query execution against Databricks

- JDBC connection pool (`DatabricksJdbcExecutor`) with configurable size and timeout
- Sets `ApplicationName` and `User` on connection for Databricks query history
- Configurable `fetchSize` and `maxRows`
- Streams `ResultSet` rows as text-encoded `DataRow` messages, flushing every 256 rows

### Trace logging

- `TableauTraceLogger` — structured SLF4J with MDC fields `session_id` and `client`
- Detects `application_name='Tableau…'` for `client=tableau` tagging
- Optional `.jsonl` corpus writer per session under `testdata/tableau-traces/`

---

## 3. What Is Not Yet Implemented

### Structural (architecture-level)

- **Gateway→server delegation:** the gateway calls Databricks directly; `skadi-server` is never invoked. The HTTP API contract exists (`docs/openapi-skadi-query-v1.yaml`) but the call is not made.
- **`skadi-core` as shared library:** normalisation (`SqlNormalizer`), hashing (`QueryCacheKey`, `QueryResultCacheKey`), and Arrow utilities (`JdbcArrowStreamer`) are in the wrong modules or duplicated.
- **Dataset versioning:** no component anywhere detects or resolves dataset versions. The architecture requires `hash(normalized_sql, parameters, dataset_version)` in every cache key; dataset_version is absent from all current implementations.

### Cache (architecture-level)

- **Arrow storage format:** primary cache path stores text rows (`List<String[]>`), not Arrow RecordBatches
- **Disk cache layer:** not implemented anywhere
- **S3 cache reachable from gateway:** exists in `skadi-server`; unreachable from gateway
- **Concurrent cache-miss lock:** no mechanism to prevent duplicate Databricks execution when multiple sessions miss the same key simultaneously

### Protocol

- `CancelRequest` — Tableau may send this; currently ignored
- Bind parameter values from `Bind (B)` message are discarded; queries with `$n` markers execute with literal markers against Databricks

### Production hardening (Lane B)

- No TLS
- No per-user query timeout or concurrency cap
- No Prometheus / OpenTelemetry metrics
- No audit log
- No Tableau Server / Cloud deployment packaging

---

## 4. Tableau SQL Endpoint Status (Lanes A and B)

### Lane A — POC

| Story | Description | Status |
|---|---|---|
| A1 | `skadi-sql-gateway` module scaffold | ✅ |
| A2 | PostgreSQL wire-protocol listener | ✅ |
| A3 | Databricks SQL Warehouse executor | ✅ |
| A4 | SQL dialect bridge (PG/MySQL → Databricks) | ✅ |
| A5 | `information_schema` facade + metadata cache | ✅ |
| A6 | Result-set typing + streaming | ✅ |
| A7 | Caching layer integration (POC) | ✅ |
| A8 | Trace harness for Tableau query patterns | ✅ |
| A9 | POC demo workbook + runbook | ✅ |

### Lane B — Production Hardening

| Story | Description | Status | Notes |
|---|---|---|---|
| B1 | Auth & authorisation (enterprise-ready) | ✅ Done | trust / plaintext / bcrypt + schema ACL |
| B2 | Cancellation, timeouts, resource controls | ❌ Not started | No `CancelRequest`; no per-user concurrency cap |
| B3 | Protocol completeness for JDBC ecosystem | ⚠️ Partial | Bind params discarded; `CancelRequest` missing |
| B4 | Correctness test suite (golden results vs Databricks) | ❌ Not started | |
| B5 | Observability — metrics, tracing, dashboards | ❌ Not started | Only basic hit/miss counters |
| B6 | Security hardening — TLS, redaction, audit log | ❌ Not started | Plaintext credentials on wire |
| B7 | Tableau Server / Cloud deployment readiness | ❌ Not started | No Helm chart; local dev only |
| B8 | MySQL wire-protocol endpoint (optional) | ❌ Not started | Dialect translator exists; no MySQL server |

---

## 5. Current Query Execution Flow

```
Client TCP connect
  → PgWireServer accepts; spawns PgWireSession thread

PgWireSession startup:
  → SSLRequest → 'N'
  → StartupMessage (protocol 3.0)
  → Optional cleartext password auth
  → PrincipalPolicy resolved for user
  → AuthOk + ParameterStatus + ReadyForQuery

Per-message loop:

  Simple Query (Q):
    1. MetadataQueryRouter.tryAnswer(sql)
         → if matched: synthetic RowDescription + DataRows; done
    2. Bootstrap intercepts (SET/RESET/SHOW/SELECT 1/version()…)
         → inline response; done
    3. SqlExecutorProvider present?
         → streamJdbcQueryWithCaching(sql); done
    4. Else → ErrorResponse "not supported"

  Extended Query (Tableau / JDBC):
    Parse (P)   → store lastPreparedSql; ParseComplete
    Bind  (B)   → store portal; BindComplete
                  *** bind parameter values discarded ***
    Describe(D) → ParameterDescription(0) + generic RowDescription/NoData
    Execute (E) → handleExecute(lastPreparedSql)
                    → same dispatch as Simple Query above
    Sync  (S)   → ReadyForQuery

streamJdbcQueryWithCaching(sql):
  1. If cache.enabled and SELECT-like:
       key = QueryCacheKey.of(rawSql, user)
         = SHA-256("user=<u>\nsql=<normalized_sql>\n")
         [no parameters, no dataset version]
       CACHE HIT  → replayFromCache (text rows); return
       CACHE MISS → continue

  2. JDBC Connection from DatabricksJdbcExecutor (own pool)
  3. Statement.execute(rawSql)
       *** rawSql is NOT passed through SqlDialectBridge ***
       *** PG-specific syntax may reach Databricks unchanged ***
  4. Stream ResultSet → DataRow messages (text encoding)
  5. Store rows in QUERY_CACHE
```

---

## 6. Current Cache Behaviour

Three independent caching mechanisms operate simultaneously inside `skadi-sql-gateway`:

### Path 1 — PgWireSession built-in (always active)

- **Scope:** `private static final` JVM singleton; shared across all sessions
- **Key:** `SHA-256("user=<u>\nsql=<normalizedSql>\n")` — no parameters, no dataset version
- **Storage:** `List<ColumnMeta>` + `List<String[]>` text rows
- **TTL:** from `SqlGatewayProperties.Cache.effectiveTtl()` (default 5 min)
- **Max entries:** hardcoded 500 — `SqlGatewayProperties.Cache.maxEntries` is never read
- **Config binding:** ignores `QueryResultCacheConfig` Spring bean entirely

### Path 2 — PgWireRowSetCache (likely dead code)

- **Scope:** Spring bean via `PgWireRowSetCacheWiring`
- **Key:** constructed in `PgWireSessionRowSetCacheBridge`
- **Storage:** `String[]` columns + `List<String[]>` rows
- **TTL:** hardcoded 2 min inside `PgWireRowSetCache`
- **Status:** no clear activation path while path 1 is always active; may double-cache results with a different TTL

### Path 3 — PgWireQueryCachingExecutorProvider (conditional)

- **Condition:** `@ConditionalOnBean(databricksDataSource)` + `cache.enabled=true`
- **Key:** `SHA-256("user=<u>\nsql=<normalizedSql>\nparams=<params>\n")` — includes parameters; no dataset version
- **Storage:** raw Arrow IPC bytes
- **TTL:** from `CacheProperties.queryResultTtl` (default 2 min)
- **SQL path:** the **only** path that applies `SqlDialectBridge` before execution
- **Config binding:** uses `CacheProperties`, a separate record from `SqlGatewayProperties.Cache`

### Config split

Two config records both bind to `skadi.sql-gateway.cache` with different fields:

| Record | `ttl` | `maxEntries` | `queryResultTtl` | Used by |
|---|---|---|---|---|
| `SqlGatewayProperties.Cache` | ✅ | ✅ (ignored) | ❌ | Path 1 |
| `CacheProperties` | ❌ | ❌ | ✅ | Path 3 + `QueryResultCacheConfig` |

Setting `ttl` in YAML affects path 1. Setting `queryResultTtl` affects path 3. They do not share state.

### What the architecture requires

Per `ai/skadi-cache-architecture.md` and `ai/skadi-architecture-diagram.md`:

- One cache: `hash(normalized_sql, parameters, dataset_version)`
- Storage format: Arrow RecordBatch streams
- Hierarchy: memory → disk → S3
- Owned by `skadi-server`; shared across nodes via S3

None of this is fully in place. The S3 tier exists in `skadi-server` and is unreachable from the gateway.

---

## 7. Known Limitations and Incomplete Areas

| # | Area | Detail |
|---|---|---|
| L1 | Bind parameters discarded | Extended-query `Bind (B)` values are parsed and silently dropped. Parameterised Tableau queries execute with literal `$1`/`$2` markers reaching Databricks. |
| L2 | Dialect bridge bypassed on primary path | `streamJdbcQueryWithCaching` sends raw client SQL to Databricks. PG-specific syntax (casts, quoted identifiers) may cause execution errors. |
| L3 | Dataset version absent from cache keys | Stale results will be served if upstream data refreshes within the TTL window. No version-aware invalidation exists. |
| L4 | Three parallel cache implementations | Different TTLs, different key schemes, different storage formats. Behaviour under concurrent load is hard to reason about. |
| L5 | Cache config has two conflicting records | `SqlGatewayProperties.Cache` and `CacheProperties` bind to the same YAML prefix; different fields are silently ignored. |
| L6 | Static cache singletons ignore Spring config | `PgWireSession.QUERY_CACHE` ignores `maxEntries` from config; `METADATA_CACHE` ignores `Metadata.ttl`; both ignore the Spring-managed `QueryResultCacheConfig` bean. |
| L7 | Metadata config not wired | `pgDatabase`, `dbxCatalog`, `dbxSchema` are string literals in `PgWireSession` constructor, not read from `SqlGatewayProperties.Metadata`. |
| L8 | `CancelRequest` not handled | Tableau sends cancel on user interrupt; currently ignored. |
| L9 | No TLS | Credentials transmitted in cleartext. |
| L10 | Gateway is not thin | Embeds its own JDBC pool, normalisation, and cache instead of delegating to `skadi-server`. Adding production features (timeouts, observability, distributed cache) requires doing so in both services. |
| L11 | `JdbcArrowStreamer` duplicated | Independent copies in `skadi-core` and `skadi-server`; changes diverge. |
| L12 | `PgWireRowSetCache` appears to be dead code | Superseded by path 1; no clear path to activation; adds maintenance surface. |
| L13 | `ai/query-flow.md` missing | Referenced in `ai/claude-instructions.md`; file does not exist. |