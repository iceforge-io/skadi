# Skadi — Development Status

> Updated: 2026-03-15
> Branch: `main`
> Last commit: `c290e32`
> Build: ✅ 115 tests passing

---

## Lane A — POC (Complete)

| Story | Description | Status | Commit |
|---|---|---|---|
| A1 | `skadi-sql-gateway` module scaffold | ✅ | early |
| A2 | PostgreSQL wire-protocol listener (minimal) | ✅ | early |
| A3 | Databricks SQL Warehouse executor | ✅ | early |
| A4 | SQL dialect bridge (PG/MySQL → Databricks) | ✅ | early |
| A5 | `information_schema` facade + metadata cache | ✅ | early |
| A6 | Result-set typing + streaming | ✅ | early |
| A7 | Caching layer integration (POC) | ✅ | `70a46b6` |
| A8 | Trace harness for Tableau query patterns | ✅ | early |
| A9 | POC demo workbook + runbook | ✅ | `946d41b` |

---

## Lane B — Production Hardening

| Story | Description | Status | Commit | Notes |
|---|---|---|---|---|
| B1 | Auth & authorisation (enterprise-ready) | ✅ Done | `adda9fd` | trust / plaintext / bcrypt + schema ACL |
| B2 | Cancellation, timeouts, resource controls | ✅ Done | `5c07c1b` | See below |
| B3 | Protocol completeness for JDBC ecosystem | ⚠️ Partial | `90f4a0b` | Bind params still discarded (L1); rest improved |
| B4 | Correctness test suite (golden results) | ❌ Not started | — | |
| B5 | Observability — metrics, tracing, dashboards | ❌ Not started | — | Hit/miss counters only |
| B6 | Security hardening — TLS, redaction, audit log | ❌ Not started | — | Plaintext credentials on wire |
| B7 | Tableau Server / Cloud deployment readiness | ❌ Not started | — | Local dev only |
| B8 | MySQL wire-protocol endpoint (optional) | ❌ Not started | — | Dialect translator exists |

---

## B2 Implementation Summary

**Commit:** `5c07c1b`
**Issue:** #24 (closed)

### Per-user concurrency cap
- `QueryGovernor`: `ConcurrentHashMap<String, AtomicInteger>` with CAS loop
- Wraps both execution paths; returns `53300` error if cap reached
- Config: `skadi.sql-gateway.pgwire.max-concurrent-queries-per-user` (0 = unlimited)

### Query timeout enforcement
- Path 1 (JDBC text rows): `Statement.setQueryTimeout(seconds)`
- Path 3 (Arrow/caching): `PreparedStatement.setQueryTimeout(seconds)`
- Config: `skadi.sql-gateway.pgwire.query-timeout` (e.g. `5m`; omit = no limit)

### Cancel propagation
- `CancelRequest` (80877102) handled via `SessionRegistry` pid/secretKey lookup
- `cancelActiveQuery()` sets `AtomicBoolean cancelFlag` + calls `Statement.cancel()`
- `cancelFlag::get` passed as `BooleanSupplier` to `JdbcArrowStreamer.stream()`
- Arrow streaming aborts immediately on cancel (Path 3)

### Tests added
- `QueryGovernorTest` — 6 cases: unlimited mode, per-user limit, release, null user

---

## B3 Partial Implementation Summary

**Commit:** `90f4a0b`
**Issue:** #25 (open)

### Completed
- `MetadataQueryRouter` extended: `pg_catalog.pg_namespace/database/type/proc/class/attribute/roles`, `current_database()`, `current_schema()`, `current_user`, `session_user`
- `ArrowIpcRowWriter`: Arrow IPC → pgwire `RowDescription` + `DataRow` streaming
- `SessionRegistry` + `BackendKeyData` with unique pid/secretKey per session
- `ParameterStatus` additions: `integer_datetimes`, `IntervalStyle`
- `SHOW` handler extended: `server_version_num`, `transaction_isolation`, `search_path`, `max_connections`, `integer_datetimes`, `intervalstyle`, `application_name`, `in_hot_standby`
- Dead code removed: `PgWireRowSetCache`, `PgWireRowSetCacheWiring`, `PgWireSessionRowSetCacheBridge`
- Metadata config wired from `SqlGatewayProperties.Metadata` (fixes L7)

### Still open (L1)
- Bind parameter values from `Bind (B)` message are discarded
- Parameterised queries execute with literal `$1`/`$2` markers reaching Databricks
- Affects Tableau extended-query data fetches with filter parameters

---

## Known Limitations — Current Open Items

| # | Area | Detail |
|---|---|---|
| L1 | Bind parameters discarded | `Bind (B)` parameter values silently dropped; `$n` markers reach Databricks |
| L2 | Dialect bridge bypassed on path 1 | Path 1 sends raw client SQL; PG-specific syntax may fail on Databricks |
| L3 | Dataset version absent from cache keys | Stale results if upstream data refreshes within TTL |
| L5 | Cache config split | `SqlGatewayProperties.Cache` and `CacheProperties` bind same YAML prefix; fields silently ignored |
| L6 | Static cache singletons ignore Spring config | `QUERY_CACHE` ignores `maxEntries`; `METADATA_CACHE` ignores `Metadata.ttl` |
| L9 | No TLS | Credentials in cleartext (B6) |
| L10 | Gateway is not thin | Embeds own JDBC pool + cache instead of delegating to `skadi-server` |
| L11 | `JdbcArrowStreamer` duplicated | Independent copies in `skadi-core` and `skadi-server` |
| L13 | `ai/query-flow.md` missing | Referenced in `ai/claude-instructions.md`; file does not exist |

---

## Next Recommended Issue

**B3 — Protocol completeness (L1: Bind parameter values)**

The single highest-value remaining B3 item. Tableau uses the extended-query protocol (`Parse → Bind → Execute`) for all data queries, and the `Bind (B)` message carries parameter values (`$1`, `$2`, …) that are currently discarded. This means:
- Any Tableau filter that produces a parameterised query silently sends literal markers to Databricks, causing execution errors or wrong results
- All other B3 work (metadata compat, cancel, SHOW extensions) is done

**Scope of L1 fix:**
1. Parse the `Bind (B)` message format codes + parameter bytes
2. Store bound values alongside `lastPreparedSql`
3. Pass them as `List<SqlParam>` into `streamJdbcQueryWithCaching()`; path 3 already accepts `List<SqlParam>` via `executeToStream()`; path 1 needs `PreparedStatement` + `ps.setObject()` instead of `Statement.execute()`
4. Update `ParameterMarkerRewriter` to reindex `$n` → `?` (already exists in `dialect` package)
5. Add integration test: parameterised query via extended-query protocol
