# Skadi — Current Implementation

> Written: 2026-03-15
> Based on: codebase audit of `main` branch at commit `09ef4a0`

---

## 1. Deployed Architecture

```
Tableau / psql / DBeaver
        │
        │  PostgreSQL wire protocol (TCP :15432)
        ▼
┌────────────────────────────────────────────────────────┐
│  skadi-sql-gateway  (Spring Boot, port 8090 HTTP)      │
│                                                        │
│  PgWireServer ──► PgWireSession (one thread per conn)  │
│       │                                                │
│       ├─ SSLRequest → 'N' (no TLS)                     │
│       ├─ StartupMessage + optional cleartext password  │
│       ├─ Auth: trust / plaintext / bcrypt              │
│       ├─ ACL: PrincipalPolicy (per-user schema filter) │
│       │                                                │
│       ├─ MetadataQueryRouter (intercepted queries)     │
│       │       └─ MetadataCache (TTL 2m, static)        │
│       │                                                │
│       └─ streamJdbcQueryWithCaching                    │
│               ├─ QueryResultCache (TTL, static)        │
│               └─ SqlExecutorProvider → Databricks JDBC │
│                       └─ SQL dialect bridge            │
│                                                        │
│  HTTP endpoints: /actuator/health, /ping, /info        │
└────────────────────────────────────────────────────────┘
        │
        │  Databricks JDBC (HTTPS)
        ▼
  Databricks SQL Warehouse

─────────────────────────────────────────────────────────

┌──────────────────────────────────────────────────────┐
│  skadi-server  (separate Spring Boot service)        │
│                                                      │
│  Unrelated to skadi-sql-gateway in current state.    │
│  Contains its own: HTTP query API, S3 cache tier,    │
│  async query materialization, peer-cache replication,│
│  JDBC SPI, dashboard UI, H2 demo mode.               │
└──────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────┐
│  skadi-core  (shared library, minimal)             │
│  Contains only: JdbcArrowStreamer                  │
│  (also duplicated inside skadi-server)             │
└────────────────────────────────────────────────────┘
```

---

## 2. Major Components and Responsibilities

### skadi-sql-gateway

| Package | Class(es) | Responsibility |
|---|---|---|
| `pgwire` | `PgWireServer` | TCP `ServerSocket` on configured port; one acceptor thread; spawns a cached thread per connection |
| `pgwire` | `PgWireSession` | Runnable per connection; full pgwire message loop; auth, query dispatch, result streaming, caching, tracing |
| `pgwire` | `JdbcToPgTypeMapper` | Maps JDBC `java.sql.Types` → PostgreSQL OIDs for `RowDescription` messages |
| `pgwire` | `PgRowWriter` | Writes `DataRow` wire messages (text format only) |
| `pgwire` | `SqlExecutorProvider` / `SqlExecutorProviderHolder` | Interface + static holder; allows Spring to inject the JDBC executor at runtime without PgWireSession needing a Spring context |
| `pgwire` | `PgWireQueryCachingExecutorProvider` | Alternative executor path that caches results as raw Arrow bytes; wired via `PgWireQueryCachingWiring` when `databricksDataSource` bean + `cache.enabled=true` |
| `pgwire` | `PgWireRowSetCache` / `PgWireRowSetCacheWiring` | Earlier row-set cache implementation (wired via `PgWireSessionRowSetCacheBridge`); stores `String[]` rows with TTL |
| `auth` | `AuthProviderFactory` | Creates `AllowAllAuthProvider`, `PlaintextAuthProvider`, or `BcryptAuthProvider` from config `auth.mode` |
| `auth` | `PrincipalPolicyRegistry` | Resolves `PrincipalPolicy` per username from `auth.policies` config; controls which schemas a user may see in metadata results |
| `dialect` | `SqlDialectBridge` | Entry point for SQL translation; delegates to `PostgresToDatabricksTranslator` or `MySqlToDatabricksTranslator` |
| `dialect` | `PostgresToDatabricksTranslator` | `::cast` → `CAST()`, `"ident"` → `` `ident` ``, `$n` markers → `?` with reordering |
| `dialect` | `MySqlToDatabricksTranslator` | `LIMIT/OFFSET` reordering, parameter swap when needed |
| `dialect` | `SqlNormalizer` | Strips comments, collapses whitespace, normalizes punctuation and casing for stable cache keys |
| `dialect` | `ParameterMarkerRewriter` | Rewrites `$1`/`$2` Postgres markers to positional `?` markers, handling out-of-order and repeated references |
| `metadata` | `MetadataQueryRouter` | Pattern-matches incoming SQL against known `information_schema` and `pg_catalog` queries; returns synthetic `MetadataRowSet` |
| `metadata` | `MetadataCache` | TTL cache for metadata rowsets keyed by SQL string |
| `cache` | `QueryResultCache` | TTL in-memory cache for both row-based results (`List<ColumnMeta>` + `List<String[]>`) and Arrow byte payloads |
| `executor` | `DatabricksJdbcExecutor` | JDBC `DataSource` wrapper for Databricks; connection pool; captures Databricks query IDs from `SqlState` |
| `trace` | `TableauTraceLogger` | Structured SLF4J logging with MDC (`session_id`, `client`); optional `.jsonl` corpus writer to `testdata/tableau-traces/` |
| `config` | `SqlGatewayProperties` | Top-level config record; sub-records for `PgWire`, `Metadata`, `Cache`, `Trace` |
| `api` | `HealthController`, `PingController`, `InfoController` | Spring MVC HTTP endpoints |

### skadi-server (independent service)

`skadi-server` is a separate Spring Boot application that pre-dates `skadi-sql-gateway`. It has no shared runtime connection to the gateway. Key capabilities:

- **HTTP query API** (`/api/v1/query`) — submit SQL, async materialize to S3, retrieve Arrow chunks
- **S3 cache tier** — `AwsSdkS3AccessLayer`, `CachedAwsSdkS3AccessLayer`, `ResultSetToS3ChunkWriter`; stores Arrow-serialized chunks in S3
- **Peer-cache replication** — `PeerCacheController` / `PeerCacheClient`; nodes can serve each other's cached results
- **JDBC SPI** — pluggable `JdbcConnectionProvider` registry
- **Dashboard UI** — monitoring controller for cache stats
- **H2 demo mode** — local in-process database for development

### skadi-core (shared library)

Contains only `JdbcArrowStreamer` — a utility that streams a JDBC `PreparedStatement` result into an Apache Arrow `IPC` byte stream. Also present (duplicated) inside `skadi-server`.

---

## 3. How Query Execution Currently Works

### Connection lifecycle

```
Client TCP connect
  → PgWireServer.accept()
  → new PgWireSession(socket, ...) submitted to cached thread pool
  → PgWireSession.run()
       ├─ Read startup packet
       ├─ Handle SSLRequest → 'N'
       ├─ Read StartupMessage (protocol 3.0)
       ├─ Auth exchange (trust / cleartext password)
       ├─ Resolve PrincipalPolicy for user
       ├─ Write AuthOk + ParameterStatus messages + ReadyForQuery
       └─ Message loop
```

### Simple Query path (`Q` message — used by psql)

```
PgWireSession.handleSimpleQuery(sql)
  1. MetadataQueryRouter.tryAnswer(sql)
     → if matched: write synthetic RowDescription + DataRows from cache/facade
  2. Bootstrap intercepts (SET, RESET, SHOW, SELECT 1, version(), current_setting())
     → inline hardcoded responses, never reaches Databricks
  3. SqlExecutorProvider present?
     → streamJdbcQueryWithCaching(out, executorProvider, sql)
  4. Otherwise → ErrorResponse "Query not supported"
```

### Extended Query path (`P/B/D/E/S` messages — used by Tableau, JDBC drivers)

```
Parse ('P')  → store sql as lastPreparedSql; reply ParseComplete
Bind  ('B')  → store portal name; reply BindComplete
              (bind parameters are parsed but NOT forwarded to JDBC — see gaps)
Describe('D') → reply ParameterDescription(0 params) + RowDescription/NoData
Execute ('E') → handleExecute(sql)
                  ├─ MetadataQueryRouter.tryAnswer
                  └─ handleSimpleQuery (reuses simple path)
Sync  ('S')  → ReadyForQuery
```

### JDBC execution path (`streamJdbcQueryWithCaching`)

```
1. Cache lookup (if cache.enabled && SELECT-like query)
   key = QueryCacheKey.of(sql, user)   // normalized_sql|user
   hit → replayFromCache (RowDescription from cached ColumnMeta + DataRows)
   miss → continue

2. Acquire JDBC Connection from SqlExecutorProvider
3. Set ApplicationName + User on connection (Databricks query history)
4. Set fetchSize and maxRows from config
5. Statement.execute(sql)
   → note: sql is NOT passed through SqlDialectBridge here
           (dialect bridge is only used in PgWireQueryCachingExecutorProvider)
6. Stream ResultSet rows as text DataRow messages
   → flush every 256 rows
   → buffer all rows into List<String[]> for cache store
7. Write CommandComplete
8. Store in QUERY_CACHE (if cacheEnabled)
```

**Important:** The primary `PgWireSession` path (steps 2–8 above) executes the raw SQL received from the client without passing it through the `SqlDialectBridge`. SQL translation only happens inside `PgWireQueryCachingExecutorProvider`, which is the secondary cache path wired via Spring when both a `databricksDataSource` bean and `cache.enabled=true`.

---

## 4. How Caching Currently Works

There are **three parallel caching mechanisms** in `skadi-sql-gateway`, all in use simultaneously:

### Cache path 1 — PgWireSession built-in (primary, active for all connections)

```
Location:    PgWireSession (static fields)
Store:       QueryResultCache QUERY_CACHE = new QueryResultCache(Clock.systemUTC(), 500)
             MetadataCache   METADATA_CACHE = new MetadataCache(Clock.systemUTC())
Key:         QueryCacheKey.of(sql, user)  → SHA-256 of "raw_sql|user"
Value:       List<ColumnMeta> + List<String[]> rows
TTL:         SqlGatewayProperties.Cache.effectiveTtl() (default 5m)
Max entries: hardcoded 500 (ignores SqlGatewayProperties.Cache.maxEntries)
Scope:       JVM-static; shared across all sessions; survives connection churn
             NOT managed by Spring; ignores QueryResultCacheConfig bean
Metadata:    MetadataCache also static; TTL hardcoded to 2m; ignores Metadata.ttl from config
```

### Cache path 2 — PgWireRowSetCache (secondary, Spring-wired)

```
Location:    PgWireRowSetCacheWiring → PgWireSessionRowSetCacheBridge (static holder)
Store:       PgWireRowSetCache (separate ConcurrentHashMap)
Key:         constructed in PgWireSessionRowSetCacheBridge
Value:       String[] columns + List<String[]> rows + commandTag
TTL:         default 2m hardcoded in PgWireRowSetCache
Scope:       Spring bean; only active when explicitly wired
Status:      Appears to be an earlier iteration; superseded by cache path 1
             May conflict/double-cache with path 1
```

### Cache path 3 — PgWireQueryCachingExecutorProvider (tertiary, Arrow bytes)

```
Location:    PgWireQueryCachingWiring → PgWireSessionCachingBridge (static holder)
Condition:   @ConditionalOnBean(databricksDataSource) + cache.enabled=true
Store:       QueryResultCache.arrowMap (added in this session's fix)
Key:         QueryResultCacheKey.cacheId(userScope, normalizedSql, normalizedParams)
Value:       raw Arrow IPC bytes (ByteArrayOutputStream)
TTL:         CacheProperties.queryResultTtl (default 2m)
SQL path:    passes sql through SqlDialectBridge BEFORE execution
Scope:       Spring bean; uses the injected QueryResultCache (not the static one)
```

### Cache configuration split

There are **two separate config records** for caching, bound to overlapping config paths:

| Config record | Prefix | Used by |
|---|---|---|
| `SqlGatewayProperties.Cache` | `skadi.sql-gateway.cache` | `PgWireSession` (path 1) |
| `CacheProperties` | `skadi.sql-gateway.cache` | `QueryResultCacheConfig`, `PgWireQueryCachingWiring` (path 3) |

Both bind to the same YAML prefix but have different fields, leading to silent divergence. `SqlGatewayProperties.Cache` has `ttl` and `maxEntries`; `CacheProperties` has `metadataTtl` and `queryResultTtl` but no `maxEntries`.

### Metadata cache

`MetadataQueryRouter` intercepts queries matching `information_schema.schemata/tables/columns` and `pg_catalog` probes. Responses are generated from static config values (`pgDatabase`, `dbxCatalog`, `dbxSchema`) hardcoded in the `PgWireSession` constructor — **not wired from `SqlGatewayProperties.Metadata`** yet.

---

## 5. Known Gaps Relative to the Intended Design

### G1 — SQL dialect bridge not applied in primary execution path

**Plan:** All client SQL is translated PG→Databricks before execution.
**Reality:** `PgWireSession.streamJdbcQueryWithCaching` executes raw client SQL directly via JDBC. Only `PgWireQueryCachingExecutorProvider` (path 3, conditionally active) uses `SqlDialectBridge`. Most connections will run untranslated SQL against Databricks.

### G2 — Bind parameters not forwarded to JDBC

**Plan:** Parameter markers `$1`, `$2` are rewritten to `?` and bound correctly.
**Reality:** The `Bind` (`B`) message handler reads and discards bind parameter values. `handleExecute` passes `lastPreparedSql` (the original statement text with `$n` markers) to `handleSimpleQuery` unchanged. No `PreparedStatement` with bound values is used. Story A4's `ParameterMarkerRewriter` exists but is not wired into the live session path.

### G3 — Three parallel cache implementations

**Plan:** One query-result cache with configurable TTL and max-entries.
**Reality:** `PgWireRowSetCache`, `QueryResultCache` (static in `PgWireSession`), and `PgWireQueryCachingExecutorProvider` (Arrow bytes via `QueryResultCache.arrowMap`) all operate independently. A query may be cached in multiple stores simultaneously with different TTLs and key schemes.

### G4 — Static singletons bypass Spring configuration

**Plan:** Cache TTL, max-entries, and metadata mapping driven by `application.yml`.
**Reality:** `QUERY_CACHE`, `METADATA_CACHE`, and `CACHE_METRICS` in `PgWireSession` are `private static final` fields constructed at class-load time with hardcoded values. The `max-entries: 500` in `SqlGatewayProperties.Cache.effectiveMaxEntries()` is unused. Metadata config (`pgDatabase`, `dbxCatalog`, `dbxSchema`) is hardcoded as string literals in `PgWireSession`'s constructor rather than read from `SqlGatewayProperties.Metadata`.

### G5 — Two conflicting cache config records

**Plan:** One config surface for cache.
**Reality:** `SqlGatewayProperties.Cache` and `CacheProperties` both bind to `skadi.sql-gateway.cache` with different field shapes. `SqlGatewayProperties.Cache` has `ttl`/`maxEntries`; `CacheProperties` has `metadataTtl`/`queryResultTtl`. Setting `ttl` in YAML affects path 1; setting `queryResultTtl` affects path 3.

### G6 — `skadi-server` S3 cache tier not integrated with pgwire gateway

**Plan (B-lane):** A distributed S3 cache tier sits behind the local in-memory cache.
**Reality:** `skadi-server` has a complete S3 cache implementation (`CachedAwsSdkS3AccessLayer`, `ResultSetToS3ChunkWriter`, peer-cache replication) but there is no bridge or shared dependency from `skadi-sql-gateway` to `skadi-server`. These are independent applications.

### G7 — `skadi-server` module role undefined

**Plan:** `skadi-server` is described as a REST/management layer placeholder.
**Reality:** It is a complete, independent service with its own HTTP query API, async query materialization, S3 integration, and JDBC SPI. Its relationship to `skadi-sql-gateway` is unspecified — they do not share code (except through the parent POM) and have no runtime interaction.

### G8 — `JdbcArrowStreamer` duplicated

**Reality:** The class exists independently in both `skadi-core` and `skadi-server` with no shared dependency. Changes to one do not propagate to the other.

### G9 — `PgWireRowSetCache` is likely dead code

`PgWireRowSetCache` and `PgWireRowSetCacheWiring` define an earlier cache iteration. With `PgWireSession`'s built-in `QUERY_CACHE` (path 1) always active and `PgWireQueryCachingExecutorProvider` (path 3) handling the Spring-wired path, `PgWireRowSetCache` has no clear activation path and duplicates the row-buffering logic.

### G10 — No TLS, no cancellation, no per-user concurrency limits

These are explicitly documented as B-lane items (B2, B6) and remain unimplemented. Relevant for any deployment beyond local development.

---

## Summary Table

| Area | Plan | Current State | Gap? |
|---|---|---|---|
| pgwire protocol (PG 3.0) | Full Simple + Extended Query | ✅ Implemented | — |
| SSL/TLS | B6 (prod) | ❌ None; returns 'N' | Expected for POC |
| Auth | trust / bcrypt / plaintext | ✅ All three modes | — |
| Schema ACL (PrincipalPolicy) | Per-user schema filter | ✅ Implemented | — |
| SQL dialect bridge | All queries translated | ⚠️ Only in cache path 3; primary path bypasses bridge | **G1** |
| Bind parameters | `$n` → `?`, bound correctly | ⚠️ Parameters discarded; raw SQL executed | **G2** |
| Metadata facade | `information_schema` answered from cache | ✅ Implemented | — |
| Metadata config wiring | Driven by `SqlGatewayProperties.Metadata` | ⚠️ Hardcoded in `PgWireSession` constructor | **G4** |
| Query result cache | Single TTL cache, keyed by normalizedSql+user | ⚠️ Three parallel implementations | **G3, G5** |
| Cache TTL from config | `application.yml` driven | ⚠️ Static singletons ignore config | **G4** |
| S3 cache tier | B-lane (distributed) | ❌ Exists in `skadi-server`, not connected | **G6** |
| Trace logging | `TableauTraceLogger` + `.jsonl` corpus | ✅ Implemented | — |
| Cancellation / timeouts | B2 | ❌ Not implemented | Expected for POC |
| Concurrency limits | B2 | ❌ Not implemented | Expected for POC |
| Observability (metrics) | B5 | ⚠️ Basic hit/miss counters; no Prometheus/OTEL | Expected for POC |