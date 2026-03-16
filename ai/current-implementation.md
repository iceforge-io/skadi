# Skadi — Current Implementation State

> Last updated: 2026-03-15
> Based on: full codebase audit + architecture doc review (`ai/`, `docs/architecture/`)
> Commit: `09ef4a0`

This document is the authoritative reference for what exists in the repository today,
measured against the intended architecture described in `ai/system-map.md`,
`ai/skadi-architecture-diagram.md`, and the `docs/architecture/` series.

---

## 1. What Is Currently Implemented

### skadi-sql-gateway — fully active

The gateway is a self-contained Spring Boot service that:

- Accepts PostgreSQL wire-protocol connections on TCP port 15432
- Handles the full pgwire startup sequence: SSLRequest (`N`), StartupMessage (protocol 3.0), optional cleartext-password auth
- Dispatches Simple Query (`Q`) and a minimal subset of Extended Query (`P/B/D/E/S/C/H`)
- Serves synthetic `information_schema` and `pg_catalog` metadata from an in-memory facade
- Executes real queries against Databricks via its own embedded JDBC connection pool
- Caches query results in-process (three parallel mechanisms — see §4)
- Translates PG/MySQL SQL dialect to Databricks SQL (conditionally — see §5)
- Logs structured session/query traces; optionally writes `.jsonl` corpus files
- Enforces per-user schema ACLs via `PrincipalPolicy`
- Exposes Spring Boot Actuator health + custom `/ping` and `/info` HTTP endpoints

**Implemented pgwire messages**

| Message | Direction | Status |
|---|---|---|
| SSLRequest | client→server | ✅ Returns `N` |
| StartupMessage | client→server | ✅ |
| PasswordMessage (`p`) | client→server | ✅ cleartext only |
| AuthenticationOk (`R=0`) | server→client | ✅ |
| AuthenticationCleartextPassword (`R=3`) | server→client | ✅ |
| ParameterStatus (`S`) | server→client | ✅ |
| BackendKeyData (`K`) | server→client | ✅ |
| ReadyForQuery (`Z`) | server→client | ✅ |
| Query (`Q`) | client→server | ✅ Simple Query path |
| Parse (`P`) | client→server | ✅ stores SQL |
| Bind (`B`) | client→server | ✅ stores portal; **parameters discarded** |
| Describe (`D`) | client→server | ✅ returns generic RowDescription |
| Execute (`E`) | client→server | ✅ delegates to simple path |
| Close (`C`) | client→server | ✅ replies CloseComplete |
| Flush (`H`) | client→server | ✅ flushes output |
| Sync (`S`) | client→server | ✅ replies ReadyForQuery |
| Terminate (`X`) | client→server | ✅ closes session |
| RowDescription (`T`) | server→client | ✅ text OIDs + type mapping |
| DataRow (`D`) | server→client | ✅ text-encoded only |
| CommandComplete (`C`) | server→client | ✅ |
| ErrorResponse (`E`) | server→client | ✅ with SQLSTATE |
| EmptyQueryResponse (`I`) | server→client | ✅ |
| ParseComplete (`1`) | server→client | ✅ |
| BindComplete (`2`) | server→client | ✅ |
| CloseComplete (`3`) | server→client | ✅ |
| NoData (`n`) | server→client | ✅ |
| ParameterDescription (`t`) | server→client | ✅ always 0 params |
| CancelRequest | client→server | ❌ not implemented |

**Implemented auth modes**

| Mode | Class | Status |
|---|---|---|
| `trust` | `AllowAllAuthProvider` | ✅ |
| `password` + `plaintext` store | `PlaintextAuthProvider` | ✅ |
| `password` + `bcrypt` store | `BcryptAuthProvider` | ✅ |
| mTLS | — | ❌ B6 |
| OAuth / token | — | ❌ B6 |

**Implemented metadata intercepts**

| Query pattern | Response |
|---|---|
| `information_schema.schemata` | Synthetic row: `public` |
| `information_schema.tables` | Synthetic rows from static config |
| `information_schema.columns` | Synthetic rows from static config |
| `pg_catalog.pg_namespace` | Synthetic |
| `pg_catalog.pg_database` | Synthetic |
| `current_database()` | Inline `postgres` |
| `current_schema()` | Inline `public` |
| `SELECT version()` | Inline `Skadi SQL Gateway (pgwire)` |
| `SHOW <setting>` | Inline values for standard settings |
| `SET / RESET` | No-op `CommandComplete` |
| `SELECT current_setting(...)` | Empty string |
| `SELECT 1` | Inline response |

**SQL dialect bridge** (`SqlDialectBridge`)

| Rule | Status |
|---|---|
| PG `"ident"` → Databricks `` `ident` `` | ✅ |
| PG `expr::type` → `CAST(expr AS type)` | ✅ |
| PG `$n` markers → `?` (with reordering) | ✅ (class exists; not wired into primary path) |
| MySQL `LIMIT/OFFSET` reorder | ✅ |
| Parameter swap for `OFFSET ? LIMIT ?` | ✅ |

**SQL normalization** (`SqlNormalizer`)

| Rule | Status |
|---|---|
| Strip `--` line comments | ✅ |
| Strip `/* */` block comments | ✅ |
| Collapse whitespace | ✅ |
| Trim outer whitespace | ✅ |
| Uppercase outside string/double-quoted literals | ✅ |
| Preserve string literal content | ✅ |
| Preserve double-quoted identifier content | ✅ |
| Normalize spacing around `=`, `,`, `(`, `)` | ✅ (extra beyond spec) |

---

### skadi-server — fully active, independent service

`skadi-server` is a complete standalone Spring Boot application. It is **not called by `skadi-sql-gateway`** and has no runtime connection to the gateway. It was built independently and contains:

| Capability | Key classes |
|---|---|
| HTTP query API (`/api/v1/query`) | `QueryV1Controller`, `QueryV1Registry` |
| Async query materialization | `QueryService`, `QueryRegistry` |
| JDBC SPI (pluggable providers) | `JdbcConnectionProvider`, `JdbcClientFactory` |
| S3 access layer | `AwsSdkS3AccessLayer`, `LocalFsS3AccessLayer` |
| S3 cache tier (Arrow chunks) | `CachedAwsSdkS3AccessLayer`, `ResultSetToS3ChunkWriter` |
| Peer-cache HTTP replication | `PeerCacheController`, `PeerCacheClient`, `PeerAuth` |
| Arrow result streaming | `JdbcArrowStreamer` (duplicated from `skadi-core`) |
| Dashboard / monitoring UI | `DashboardController`, `CacheStatsController` |
| H2 demo mode | `DemoH2Config`, `DemoH2Service` |
| Cache statistics | `CacheMetrics`, `QueryStatsRegistry` |
| S3 client SPI | `S3ClientFactory`, `S3ClientProvider`, `DefaultAwsS3ClientProvider` |

---

### skadi-core — shared library, largely empty

Contains only `JdbcArrowStreamer`. Per `ai/system-map.md` and `ai/claude-instructions.md`, it should also contain: query normalization, cache key hashing, dataset versioning, Arrow utilities, and configuration models. None of these exist here yet — they have grown inside the other modules instead.

---

## 2. Which Architecture Components Already Exist

The following components from the intended architecture exist in the repository, though not always in the correct module or connected correctly:

| Architecture Component | Exists? | Location | Correct module? |
|---|---|---|---|
| pgwire TCP server | ✅ | `skadi-sql-gateway` | ✅ |
| Auth (trust/plaintext/bcrypt) | ✅ | `skadi-sql-gateway` | ✅ |
| Per-user schema ACL | ✅ | `skadi-sql-gateway` | ✅ |
| SQL dialect bridge (PG→Databricks) | ✅ | `skadi-sql-gateway` | ✅ |
| SQL normalizer | ✅ | `skadi-sql-gateway/dialect` | ❌ should be `skadi-core` |
| Cache key hashing | ✅ | `skadi-sql-gateway/cache` | ❌ should be `skadi-core` |
| `information_schema` facade | ✅ | `skadi-sql-gateway` | ✅ |
| Metadata TTL cache | ✅ | `skadi-sql-gateway` | ✅ |
| Query result cache (in-memory) | ✅ | `skadi-sql-gateway` | ❌ should be `skadi-server` |
| Trace logger + corpus writer | ✅ | `skadi-sql-gateway` | ✅ |
| Databricks JDBC executor | ✅ | `skadi-sql-gateway` | ❌ should be `skadi-server` |
| Arrow streaming (JDBC→Arrow) | ✅ | `skadi-core` + `skadi-server` (duplicated) | ❌ should be `skadi-core` only |
| S3 cache tier (Arrow chunks) | ✅ | `skadi-server` | ✅ (not yet reachable from gateway) |
| Peer-cache replication | ✅ | `skadi-server` | ✅ (not yet reachable from gateway) |
| HTTP query execution API | ✅ | `skadi-server` | ✅ (not yet called by gateway) |
| JDBC connection SPI | ✅ | `skadi-server` | ✅ (not yet used by gateway) |
| Dataset versioning | ❌ | — | — |
| Disk cache layer | ❌ | — | — |
| Concurrent cache-miss lock | ❌ | — | — |
| CancelRequest handling | ❌ | — | — |
| TLS termination | ❌ | — | — |

---

## 3. Lane B — Completion Status

Lane B stories from `ai/plan/tableau-sql-endpoint.md`:

| Story | Description | Status | Notes |
|---|---|---|---|
| **B1** | Auth & authorization (enterprise-ready) | ✅ Done | trust / plaintext / bcrypt + schema ACL |
| **B2** | Cancellation, timeouts, resource controls | ❌ Not started | No CancelRequest; no per-user concurrency cap; advisory `setMaxRows` only |
| **B3** | Protocol completeness for JDBC ecosystem | ⚠️ Partial | Extended query works for Tableau; bind parameters discarded; CancelRequest missing |
| **B4** | Correctness test suite (golden results) | ❌ Not started | No golden-result tests against Databricks; no seeded test dataset |
| **B5** | Observability (production-grade) | ❌ Not started | Basic hit/miss counters only; no Prometheus/OTEL metrics, no tracing spans, no dashboards |
| **B6** | Security hardening | ❌ Not started | No TLS; cleartext password only; no audit log; no token redaction in logs |
| **B7** | Tableau Server / Cloud deployment | ❌ Not started | No Helm chart, no deployment docs beyond local dev runbook |
| **B8** | MySQL wire-protocol endpoint (optional) | ❌ Not started | Dialect translator exists; no MySQL wire server |

---

## 4. How Caching Currently Works

There are **three parallel, independent caching mechanisms** inside `skadi-sql-gateway`.

### Cache path 1 — PgWireSession static cache (primary, always active)

```
Classes:     PgWireSession.QUERY_CACHE, PgWireSession.METADATA_CACHE
Lifecycle:   private static final — created at JVM class load, never destroyed
Key:         QueryCacheKey.of(sql, user) → SHA-256 of "user=<u>\nsql=<normalized_sql>\n"
             No parameters. No dataset version.
Value:       List<ColumnMeta> + List<String[]> rows (text-encoded)
TTL:         SqlGatewayProperties.Cache.effectiveTtl() (default 5m)
Max entries: hardcoded 500 — SqlGatewayProperties.Cache.maxEntries is never read
Config:      Uses SqlGatewayProperties.Cache for TTL only; ignores maxEntries
Spring:      Not managed by Spring; ignores QueryResultCacheConfig bean
```

### Cache path 2 — PgWireRowSetCache (likely dead code)

```
Classes:     PgWireRowSetCache, PgWireRowSetCacheWiring, PgWireSessionRowSetCacheBridge
Lifecycle:   Spring bean (optional)
Key:         Constructed in PgWireSessionRowSetCacheBridge
Value:       String[] columns + List<String[]> rows + commandTag
TTL:         hardcoded 2m inside PgWireRowSetCache
Status:      Earlier iteration; no clear activation path while path 1 is always active
             May double-cache same results with different TTL
```

### Cache path 3 — PgWireQueryCachingExecutorProvider (Arrow bytes)

```
Classes:     PgWireQueryCachingExecutorProvider, PgWireQueryCachingWiring
Condition:   @ConditionalOnBean(databricksDataSource) + cache.enabled=true
Key:         QueryResultCacheKey.cacheId(userScope, normalizedSql, params) → SHA-256
             Includes parameters. No dataset version.
Value:       Raw Arrow IPC bytes
TTL:         CacheProperties.queryResultTtl (default 2m)
SQL path:    Passes SQL through SqlDialectBridge before execution (only path that does)
Config:      Uses CacheProperties (separate config record from SqlGatewayProperties.Cache)
```

### Cache configuration split

Two separate config records both bound to `skadi.sql-gateway.cache`:

| Record | Has `ttl` | Has `maxEntries` | Has `queryResultTtl` | Used by |
|---|---|---|---|---|
| `SqlGatewayProperties.Cache` | ✅ | ✅ | ❌ | Path 1 (PgWireSession) |
| `CacheProperties` | ❌ | ❌ | ✅ | Path 3 + QueryResultCacheConfig |

Setting `ttl` in YAML affects path 1. Setting `queryResultTtl` affects path 3. The same key name maps to different fields.

### What the architecture requires

Per `ai/skadi-cache-architecture.md` and `ai/skadi-architecture-diagram.md`:
- **One** deterministic cache: `hash(normalized_sql, parameters, dataset_version)`
- Cache stores **Arrow RecordBatch streams**, not text rows
- Cache hierarchy: memory → disk → S3
- Cache owned by `skadi-server`, accessible to gateway
- None of this is present today

---

## 5. How Query Execution Currently Works

### Connection lifecycle

```
Client TCP connect → PgWireServer.accept()
  → PgWireSession.run() on cached thread pool thread
       ├── SSLRequest → 'N'
       ├── StartupMessage (protocol 3.0)
       ├── Optional cleartext password auth
       ├── PrincipalPolicy resolved for user
       ├── AuthOk + ParameterStatus + ReadyForQuery
       └── Message loop (per-message dispatch)
```

### Simple Query path (psql, some JDBC drivers)

```
handleSimpleQuery(sql)
  1. MetadataQueryRouter.tryAnswer(sql)     → synthetic response if matched
  2. Bootstrap intercepts (SET/RESET/SHOW/SELECT 1/version()...)
  3. SqlExecutorProvider present?
       → streamJdbcQueryWithCaching(sql)    → Databricks JDBC
  4. Else → ErrorResponse "not supported"
```

### Extended Query path (Tableau, JDBC)

```
Parse (P)   → store lastPreparedSql; ParseComplete
Bind  (B)   → store portal name; BindComplete
               *** bind parameter values parsed but silently discarded ***
Describe(D) → ParameterDescription(0) + generic RowDescription
Execute (E) → handleExecute(lastPreparedSql)
               → same as handleSimpleQuery
Sync  (S)   → ReadyForQuery
```

### JDBC execution (`streamJdbcQueryWithCaching`)

```
1. Cache lookup: QueryCacheKey.of(rawSql, user) → path 1 cache
   HIT  → replayFromCache (text rows, no Arrow)
   MISS → continue

2. JDBC Connection from SqlExecutorProvider (Databricks connection pool)
3. Statement.execute(rawSql)
   *** rawSql is NOT passed through SqlDialectBridge ***
   *** PG-specific syntax may reach Databricks unchanged ***

4. Stream ResultSet → PgRowWriter.writeDataRow (text encoding)
   Flush every 256 rows
   Buffer all rows for cache write

5. QUERY_CACHE.put(key, colMetas, rows, ttl)
```

**Critical:** SQL translation (`SqlDialectBridge`) is only applied in cache path 3 (`PgWireQueryCachingExecutorProvider`), which is conditionally active. The primary path executes raw client SQL directly.

---

## 6. Architecture Drift

The following table documents where the current implementation diverges from the architecture defined in `ai/system-map.md`, `ai/skadi-architecture-diagram.md`, and `ai/claude-instructions.md`.

### Structural drift (violates module responsibility boundaries)

| ID | Architecture rule | Current state |
|---|---|---|
| **D1** | `skadi-sql-gateway` is a thin protocol adapter; it calls `skadi-server` for execution | Gateway contains its own Databricks JDBC executor (`DatabricksJdbcExecutor`) and connection pool. `skadi-server` is never called. |
| **D2** | Execution and caching belong in `skadi-server` | All active caching logic (`QueryResultCache`, `PgWireRowSetCache`, metadata cache) lives inside `skadi-sql-gateway`. |
| **D3** | Query normalization and cache key hashing belong in `skadi-core` | `SqlNormalizer`, `QueryCacheKey`, `QueryResultCacheKey` all live in `skadi-sql-gateway/dialect` and `skadi-sql-gateway/cache`. |
| **D4** | `JdbcArrowStreamer` is a `skadi-core` shared utility | Exists in both `skadi-core` and `skadi-server` as independent copies. Changes diverge. |
| **D5** | `skadi-server` is the execution engine, called by the gateway | `skadi-server` is an independent service with its own HTTP API, never invoked by the gateway. No interface between the two exists. |

### Functional drift (cache key incomplete)

| ID | Architecture requirement | Current state |
|---|---|---|
| **D6** | Cache key = `hash(normalized_sql, parameters, dataset_version)` | `QueryCacheKey` (path 1): `hash(normalized_sql, user)` — missing parameters and dataset version |
| **D7** | Cache key = `hash(normalized_sql, parameters, dataset_version)` | `QueryResultCacheKey` (path 3): `hash(user, normalized_sql, params)` — missing dataset version |
| **D8** | Dataset version included in every cache key to prevent stale results | No dataset version concept exists anywhere in the codebase |

### Functional drift (cache storage format)

| ID | Architecture requirement | Current state |
|---|---|---|
| **D9** | Cache stores Arrow RecordBatch streams | Path 1 stores `List<String[]>` text rows. Path 3 stores raw Arrow IPC bytes but is a secondary path only. |
| **D10** | Cache hierarchy: memory → disk → S3 | Memory only (path 1). S3 exists in `skadi-server` but is unreachable from the gateway. No disk layer anywhere. |

### Functional drift (SQL execution)

| ID | Architecture requirement | Current state |
|---|---|---|
| **D11** | All client SQL is dialect-translated before execution | Primary execution path (`streamJdbcQueryWithCaching`) sends raw client SQL to Databricks. Translation only in secondary path 3. |
| **D12** | Bind parameters from `Bind (B)` message forwarded to JDBC `PreparedStatement` | Bind parameter values are parsed and silently discarded. Queries with `$n` markers are executed with literal markers against Databricks. |

### Configuration drift

| ID | Architecture requirement | Current state |
|---|---|---|
| **D13** | One cache config surface | Two overlapping config records (`SqlGatewayProperties.Cache` and `CacheProperties`) bind to the same YAML prefix with different field names. |
| **D14** | Static cache config driven by `application.yml` | `PgWireSession.QUERY_CACHE` and `METADATA_CACHE` are `static final` singletons ignoring Spring config for max-entries and metadata mapping. |

### Missing components (not yet implemented anywhere)

| ID | Component | Needed for |
|---|---|---|
| **M1** | Dataset version resolution (table/partition metadata from Databricks) | Correct cache key; stale-result prevention |
| **M2** | Disk cache layer | Architecture cache hierarchy |
| **M3** | Concurrent cache-miss lock | Preventing duplicate Databricks execution |
| **M4** | `skadi-server` callable interface (HTTP or internal) from gateway | Connecting the two services |
| **M5** | `ai/query-flow.md` | Referenced in `ai/claude-instructions.md`; file does not exist |

---

## 7. What Must Not Change Without Architecture Review

Per `ai/claude-instructions.md` and `ai/system-map.md`, any changes that:

1. Move execution logic into `skadi-sql-gateway` (drift gets worse, not better)
2. Add a third or fourth cache implementation without removing the existing parallel ones
3. Add a new config record for cache properties (two already conflict)
4. Duplicate shared utilities instead of placing them in `skadi-core`
5. Expand `skadi-core` to contain Spring Boot or network code

...require explicit architectural decision before proceeding.

---

## Summary: Implementation vs Architecture

| Concern | Architecture intent | Implementation state |
|---|---|---|
| pgwire protocol | thin adapter | ✅ Complete (minor gaps: no CancelRequest, bind params discarded) |
| Auth | trust / password modes | ✅ B1 complete |
| SQL dialect translation | all queries translated | ⚠️ Exists but only on secondary path |
| Bind parameter handling | forwarded to JDBC | ❌ Discarded |
| Metadata facade | synthetic `information_schema` | ✅ Works; config not wired |
| Normalization | `skadi-core` utility | ⚠️ Exists in wrong module |
| Cache key | `hash(sql, params, dataset_version)` | ⚠️ Missing dataset_version; path 1 also missing params |
| Cache storage | Arrow RecordBatches | ⚠️ Path 1 stores text rows; path 3 stores Arrow bytes |
| Cache layers | memory → disk → S3 | ⚠️ Memory only in gateway; S3 exists in server but unreachable |
| Dataset versioning | partition-aware, in cache key | ❌ Not implemented |
| Module separation | gateway calls server | ❌ Gateway is self-contained; server never called |
| Distributed cache | shared S3 across nodes | ❌ S3 exists in server; no multi-node path |
| Observability | metrics + tracing | ⚠️ Basic counters only |
| TLS | required for production | ❌ B6; not started |
| Cancellation | CancelRequest + JDBC cancel | ❌ B2; not started |
| Concurrency limits | per-user caps | ❌ B2; not started |
