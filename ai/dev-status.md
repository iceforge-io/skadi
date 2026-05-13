# Skadi — Development Status

> Updated: 2026-05-12
> Branch: `feature/tableau-sql-endpoint-b7-deployment-readiness` → PR #35
> Last commit: pending push
> Build: ✅ 201 tests passing

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
| B3 | Protocol completeness for JDBC ecosystem | ✅ Done | `0243425` (PR #31) | L1 fixed: Bind params parsed + forwarded to both execution paths |
| B4 | Correctness test suite (golden results) | ✅ Done | `ce47076` (PR #32) | 36 tests; 2 bugs fixed in Arrow/path-1 value rendering |
| B5 | Observability — production-grade | ✅ Done | `d2c7f7a` (PR #33) | Prometheus endpoint, Micrometer timers, session gauge, correlation IDs |
| B6 | Security hardening — TLS, redaction, audit log | ✅ Done | `287740e` (PR #34) | SecretRedactor, SqlSecurityValidator, AuditLog, require-ssl enforcement |
| B7 | Tableau Server / Cloud deployment readiness | ✅ Done | pending (PR #35) | Dockerfile, docker-compose, health indicator, full deployment docs |
| B8 | MySQL wire-protocol endpoint (optional) | ❌ Not started | — | Dialect translator exists |

---

## B7 Implementation Summary

**Issue:** #35 (to be closed)

### PgWireHealthIndicator
- `@Component("pgWire")` registered under `pgWire` key for `/actuator/health/pgWire`
- `@ConditionalOnProperty(prefix = "skadi.sql-gateway.pgwire", name = "enabled", havingValue = "true")`
- Reports `UP` with `port` and `protocol` details when `PgWireServerLifecycle` is running
- Reports `DOWN` with `reason` when disabled or not yet bound
- 2 unit tests: UP path (real server on ephemeral port), DOWN path (disabled lifecycle)

### Docker packaging
- `skadi-sql-gateway/Dockerfile`: multi-stage build (eclipse-temurin:21-jdk-jammy → 21-jre-jammy)
- Non-root `skadi` system user; EXPOSE 15432 and 8090; HEALTHCHECK on `/actuator/health`
- Note: Databricks JDBC driver excluded (provided/optional scope) — extend image or mount volume
- `skadi-sql-gateway/docker-compose.yml`: full env-var mapping with sensible defaults; Prometheus/Grafana commented

### Deployment documentation
- `docs/deployment/README.md`: architecture overview, port table, full config reference table, health endpoint table
- `docs/deployment/docker.md`: build, compose, docker run, JDBC driver options, TLS proxy examples, k8s manifest with readiness probe on `/actuator/health/pgWire`
- `docs/deployment/tableau-server.md`: Tableau Desktop connect walkthrough, publish data source, schedule refresh, SSL, supported features
- `docs/deployment/tableau-bridge.md`: Bridge architecture, install walkthrough, live vs extract modes, firewall requirements, sizing guide
- `docs/deployment/production-checklist.md`: infrastructure, configuration, security, health check, smoke test, observability, Tableau, Databricks, runbook reference
- `docs/deployment/troubleshooting.md`: connection errors, query errors, performance, Docker startup, Tableau-specific, logging/diagnostics guidance

### Smoke test script
- `scripts/smoke-test.sh`: tests HTTP health, pgwire connectivity, metadata facade, optional real Databricks query; exit 0 on pass / exit 1 on fail

### Module README
- `skadi-sql-gateway/README.md`: rewritten with quick start, ports, auth modes, build/test, Docker, config reference, health endpoints, links to deployment guides

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

## B3 Implementation Summary

**Commits:** `90f4a0b` (partial), pending (L1 fix)
**Issue:** #25 (closed)

### Completed
- `MetadataQueryRouter` extended: `pg_catalog.pg_namespace/database/type/proc/class/attribute/roles`, `current_database()`, `current_schema()`, `current_user`, `session_user`
- `ArrowIpcRowWriter`: Arrow IPC → pgwire `RowDescription` + `DataRow` streaming
- `SessionRegistry` + `BackendKeyData` with unique pid/secretKey per session
- `ParameterStatus` additions: `integer_datetimes`, `IntervalStyle`
- `SHOW` handler extended: `server_version_num`, `transaction_isolation`, `search_path`, `max_connections`, `integer_datetimes`, `intervalstyle`, `application_name`, `in_hot_standby`
- Dead code removed: `PgWireRowSetCache`, `PgWireRowSetCacheWiring`, `PgWireSessionRowSetCacheBridge`
- Metadata config wired from `SqlGatewayProperties.Metadata` (fixes L7)
- **L1 fixed**: `Bind (B)` message parameter values fully parsed + stored as `List<SqlParam>`
  - Format codes (text/binary) honoured; binary params fall back to UTF-8 with a warning
  - Path 3 (Arrow + caching): params forwarded to `executeToStream()` — `SqlDialectBridge` rewrites `$n→?` and binds via `PreparedStatement`
  - Path 1 (JDBC fallback): when params present, `SqlDialectBridge` rewrites SQL, `PreparedStatement` used — path-1 cache bypassed (cache key doesn't include params)
  - Parse (`P`) message param type OID count stored → `ParameterDescription` now reports accurate param count
  - `applyConnectionContext`, `applyStatementLimits`, `bindParams` extracted as helpers
  - `streamRows` extracted to avoid duplication between Statement/PreparedStatement paths
- Tests: `ExtendedQueryParameterTest` (3 raw-protocol cases: text params, null param, zero params)

---

## Known Limitations — Current Open Items

| # | Area | Detail |
|---|---|---|
| L2 | Dialect bridge bypassed on path 1 (zero-param) | Path 1b sends raw client SQL; PG-specific syntax may fail on Databricks |
| L3 | Dataset version absent from cache keys | Stale results if upstream data refreshes within TTL |
| L5 | Cache config split | `SqlGatewayProperties.Cache` and `CacheProperties` bind same YAML prefix; fields silently ignored |
| L6 | Static cache singletons ignore Spring config | `QUERY_CACHE` ignores `maxEntries`; `METADATA_CACHE` ignores `Metadata.ttl` |
| L9 | No TLS | Credentials in cleartext (B6) |
| L10 | Gateway is not thin | Embeds own JDBC pool + cache instead of delegating to `skadi-server` |
| L11 | `JdbcArrowStreamer` duplicated | Independent copies in `skadi-core` and `skadi-server` |
| L13 | `ai/query-flow.md` missing | Referenced in `ai/claude-instructions.md`; file does not exist |
| L14 | Execute sends RowDescription when Describe already did | Extended-query flow: server emits RowDescription in Execute response even after Describe(S/P) was answered. JDBC drivers that send Describe (DBeaver, DataGrip) may see duplicate `T` messages; Tableau (no Describe) is unaffected |
| L15 | Bind binary-format params not decoded | Binary-format (`fmt=1`) bind params decoded as UTF-8 text with a warning; correct decoding requires type OID dispatch |
| L16 | `SqlGatewayIT` placeholder still disabled | Real Databricks integration tests need CI secrets and a seeded test warehouse; currently placeholder only |
| L17 | `TIMESTAMPTZ` Arrow path not tested | `TimeStampMilliTZVector` codepath not exercised; Databricks returns this for `TIMESTAMP WITH TIME ZONE` columns |
| L18 | `BOOLEAN` wire format is `"true"`/`"false"` | PostgreSQL native protocol uses `"t"`/`"f"`; some strict clients may reject the lowercase word form |

---

## B4 Implementation Summary

**Issue:** #26
**Build:** 154 tests, 0 failures (36 new tests added)

### Bugs fixed

| Bug | File | Detail |
|---|---|---|
| Arrow timestamp rendered as epoch millis | `ArrowIpcRowWriter.java` | `TimeStampMilliVector.getObject()` returns `Long`; now rendered via `LocalDateTime.ofInstant(…, ZoneId.systemDefault())` |
| Arrow date rendered as epoch days | `ArrowIpcRowWriter.java` | `DateDayVector.getObject()` returns `Integer`; now rendered via `LocalDate.ofEpochDay()` |
| Path 1 timestamp trailing `.0` | `PgWireSession.java` | `Timestamp.toString()` produces `"2021-06-15 12:30:00.0"`; replaced with `renderJdbcValue()` using `Timestamp.toLocalDateTime()` |

### Test classes added

| Class | Tests | What it covers |
|---|---|---|
| `ValueEncodingArrowTest` | 8 | Arrow IPC → pgwire text rendering; timestamp, date, decimal, null, bigint |
| `NullSemanticsTest` | 4 | SQL NULL arrives as Java `null`; `wasNull()` returns true |
| `DecimalCorrectnessTest` | 5 | Scale preserved; negative sign; zero; small fractional; inline CAST |
| `TimestampFormattingTest` | 5 | No trailing `.0`; ISO format; epoch-zero renders as date not number |
| `TypeOidCorrectnessTest` | 8 | OID round-trip: BIGINT→20, INTEGER→23, NUMERIC→1700, DATE→1082, TIMESTAMP→1114, etc. |
| `OrderingLimitTest` | 6 | ASC/DESC order; LIMIT; LIMIT+OFFSET; string collation |

### Infrastructure

- `GatewayCorrectnessHarness`: shared H2-backed PgWireServer; trust auth; cache disabled; simple-query mode
- Golden results are inline expected values in each test (not snapshot files)
- No Databricks connection required — all 154 tests pass in CI

### Note on timezone (Arrow path)

`TIMESTAMP` (no TZ) round-trips through `ZoneId.systemDefault()` — consistent with JDBC's `Timestamp.toLocalDateTime()`. Both paths now produce identical output regardless of JVM timezone offset.

---

## B5 Implementation Summary

**Issue:** #27
**Build:** 167 tests, 0 failures (13 new tests)

### Dependencies added
- `micrometer-registry-prometheus` — enables `/actuator/prometheus` scrape endpoint (Micrometer core was already present via `spring-boot-starter-actuator`)

### New classes (`metrics` package)

| Class | Role |
|---|---|
| `GatewayMetrics` | Spring `@Component`; owns all Micrometer meters |
| `GatewayMetricsHolder` | Static null-safe bridge; lets `PgWireSession` (manually constructed) call metrics without Spring injection |
| `GatewayMetricsWiring` | `@Configuration`; sets `GatewayMetricsHolder` at startup |

### Metrics exposed at `/actuator/prometheus`

| Metric | Type | Tags | Description |
|---|---|---|---|
| `skadi_sessions_active` | Gauge | — | Currently connected pgwire clients |
| `skadi_queries_seconds` | Timer histogram | `cache_tier`, `outcome` | Query latency; p50/p95/p99 via percentile histogram |
| `skadi_query_errors_total` | Counter | `sqlstate` | Failed queries by SQLSTATE code |

Cache tier tag values: `hit` (path-1b cache hit), `miss` (path-1b miss), `skip` (no cache), `arrow_hit` (path-3 cache hit), `arrow_miss` (path-3 Databricks execute).

### Correlation IDs

- `query_id` (12-char hex UUID) added to MDC for each query execution
- Appears in every log line during query: `[sid=abc qid=def123 client=tableau]`
- Forwarded to Databricks via `setClientInfo("ApplicationName", "skadi/<session_id>/<query_id>")`
- Enables correlating client-visible latency with Databricks query history

### Structured log redaction

- SQL text only appears in trace mode (`trace.enabled=true`)
- Bind parameter values never logged at any level
- Passwords never logged (arrive in separate `PasswordMessage`, not startup params)
- Databricks token never referenced in logs

### Config additions (`application.yml`)

```yaml
management:
  endpoint:
    prometheus:
      enabled: true
  metrics:
    tags:
      application: skadi-sql-gateway        # applied to all metrics
    distribution:
      percentiles:
        skadi.queries: 0.5,0.95,0.99
      percentiles-histogram:
        skadi.queries: true
```

### Known gaps (B5 TODOs)

| ID | Gap | Notes |
|---|---|---|
| L19 | No Grafana dashboard template | Metrics are Prometheus-ready; dashboard JSON not yet provided |
| L20 | `QueryCacheMetrics` (path-1 static) not bridged to Micrometer | It's a `private static` in `PgWireSession`; would require exposing via accessor. Cache tier tags on `skadi_queries` provide equivalent visibility |
| L21 | No OpenTelemetry tracing spans | Structured traces via OTel would complement the MDC `query_id` for distributed tracing; not in scope without OTel dependency decision |

---

## B6 Implementation Summary

**Issue:** #28
**Build:** 199 tests, 0 failures (32 new tests)

### New classes (`security` package)

| Class | Role |
|---|---|
| `SecretRedactor` | Centralized utility: redacts sensitive keys (password, token, secret, credential, key, auth) from parameter maps. Used by `TableauTraceLogger` on startup params. |
| `SqlSecurityValidator` | Rejects queries exceeding 1 MiB or containing null bytes (protocol injection). Maps to SQLSTATE `42501`. Error messages contain no SQL content. |
| `AuditLog` | Always-on structured audit events via `skadi.audit` logger. Emits `audit_connect`, `audit_query`, `audit_error`. Never includes SQL text or parameter values. Log-injection protection via newline sanitisation. |

### `SqlGatewayProperties.PgWire.Tls` config record

```yaml
pgwire:
  tls:
    enabled: false          # true: respond 'S' to SSLRequest and upgrade (needs keystore)
    require-ssl: false      # true: reject clients that skip SSLRequest (SQLSTATE 28000)
    keystore-path: /etc/skadi/server.p12
    keystore-password: changeit
    keystore-type: PKCS12
```

- `require-ssl=true` enforcement tested: raw TCP client skipping SSLRequest is rejected with an `ErrorResponse` containing SQLSTATE `28000` and the string "SSL"
- Existing tests (no `Tls` config = null) are backward-compatible — null tls config allows all connections

### `DatabricksProperties.toString()` masking

Token value replaced with `***` in `toString()` — prevents token appearing in actuator endpoints or debug logs.

### Instrumentation in `PgWireSession`

| Hook | Event |
|---|---|
| Auth success | `AuditLog.connect(..., outcome=SUCCESS)` |
| Auth failure (bad password, wrong message type) | `AuditLog.connect(..., outcome=DENIED)` |
| require-ssl rejection | `AuditLog.connect(..., outcome=DENIED)` |
| Each query completion | `AuditLog.query(user, schema, fingerprint, cacheTier, rows, latencyMs)` |
| Each query error | `AuditLog.queryError(user, schema, fingerprint, sqlstate)` |
| Before executor call | `SqlSecurityValidator.validate(sql)` — rejects oversized/malformed |

`client = deriveClient(params)` moved before the auth check so it is available for audit logging on auth failure.

### `TableauTraceLogger` hardening

`formatParams()` now passes startup params through `SecretRedactor.redact()` before logging — belt-and-suspenders protection for clients that send unexpected sensitive keys in startup params.

### Audit logger (`logback-spring.xml`)

```xml
<logger name="skadi.audit" level="INFO" additivity="false">
    <appender-ref ref="CONSOLE"/>
</logger>
```

`additivity=false` prevents duplication in root logger. Operators can redirect to a separate file appender for SIEM integration.

### Tests (32 new, all passing)

| Class | Tests | Coverage |
|---|---|---|
| `SecretRedactorTest` | 11 | Key detection, map redaction, immutability, edge cases |
| `SqlSecurityValidatorTest` | 8 | Valid SQL, null byte, over-size, error message safety |
| `AuditLogTest` | 9 | Connect/query/error events, log-injection protection, no SQL text in output |
| `RequireSslTest` | 4 | require-ssl enforcement, null-config backward compat, raw-TCP error verification |

### Known security gaps (L22–L24)

| ID | Gap | Notes |
|---|---|---|
| L22 | STARTTLS upgrade not implemented | `SSLRequest` still returns `N` even when `tls.enabled=true`; full socket upgrade requires `SSLContext` from keystore (implementation deferred). Use stunnel/Envoy for production TLS. |
| L23 | Schema ACL denials not audited | `applyPolicyFilter()` silently removes rows; no `audit_acl_deny` event. Filter is applied at metadata query level only. |
| L24 | `keystorePassword` stored in plaintext in config | B6 acceptance criteria; use Spring's `jasypt` or env-var injection for production keystore credentials |

---

## Next Recommended Issue

**B7 — Tableau Server / Cloud deployment readiness**

B6 is complete. B7 scope:
- Helm chart or Docker Compose deployment template
- Deployment guide: TLS proxy (stunnel/Envoy) in front of gateway
- Health check / readiness probe integration
- Environment variable overrides for all secrets (Databricks token, keystore password)

**Alternative: L14 — Fix Execute/Describe RowDescription duplication**
Unlocks DBeaver/DataGrip extended-query compatibility.

---

## Technical Debt Discovered During B5

| ID | Detail | Priority |
|---|---|---|
| L19 | No Grafana dashboard template | Low — metrics are Prometheus-ready; dashboard JSON is a follow-up deliverable |
| L20 | `QueryCacheMetrics` (path-1b static singleton) not in Micrometer | Low — it is `private static final` in `PgWireSession`; `cache_tier` tags on `skadi_queries` give equivalent visibility |
| L21 | No OpenTelemetry spans | Medium — `query_id` MDC + Databricks `ApplicationName` covers correlation; OTel spans require a dep decision |

---

## Technical Debt Discovered During B4

| ID | Detail | Priority |
|---|---|---|
| L16 | `SqlGatewayIT` placeholder — real Databricks integration tests need CI secrets + seeded warehouse | Low (blocks live data validation only) |
| L17 | `TIMESTAMPTZ` Arrow path untested — `TimeStampMilliTZVector` not exercised by H2 (H2 has no `TIMESTAMP WITH TIME ZONE` that round-trips via Arrow TZ vector) | Medium (Databricks `timestamp_ltz` columns) |
| L18 | `BOOLEAN` renders as `"true"`/`"false"` — PostgreSQL native uses `"t"`/`"f"`; `JdbcToPgTypeMapper` maps BOOL→OID 16 correctly but value encoding diverges | Low (most clients accept both forms) |
