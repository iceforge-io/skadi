
### Definition of Done (Epic)
- ✅ End-to-end demo: Tableau → Skadi (PG) → Databricks → results
- ✅ Warm-cache run shows reduced warehouse load + improved latency
- ✅ Basic correctness: types, nulls, timestamps, decimal scale, row counts
- ✅ Minimal security: authentication + per-user session isolation
- ✅ Documentation: setup + known limitations + troubleshooting

---

# Lane A — POC (Make Tableau Connect + Show Caching)

## STORY A1 — Create `skadi-sql-gateway` module scaffold
**Description**
- Create a new module/service entrypoint dedicated to SQL wire protocol + JDBC execution.

**Acceptance Criteria**
- Module builds in CI.
- Service can start locally with config stubs.
- Health endpoint indicates readiness.

**Tasks**
- Add module skeleton, config loading, lifecycle.
- Add integration test harness placeholder.

---

## STORY A2 — Implement PostgreSQL wire protocol listener (minimal)
**Description**
- Implement a minimal PostgreSQL-compatible endpoint sufficient for Tableau/JDBC clients:
    - handshake/startup
    - authentication (POC simple)
    - simple query flow
    - prepared statement flow (subset)
    - error responses

**Acceptance Criteria**
- `psql` can connect and run a trivial query against the gateway (even if results are mocked first).
- Tableau Desktop can “connect” (even if schema browsing not fully working yet).

**Tasks**
- Choose/implement PG protocol library or implement minimal protocol server.
- Implement session lifecycle.
- Implement query request parsing for:
    - Simple Query (`Q`)
    - Parse/Bind/Execute (`Parse/Bind/Execute/Sync`) — minimal subset

> **POC opinion:** start with supporting the minimum messages required by Tableau + common JDBC drivers; expand as traces dictate.

---

## STORY A3 — Databricks SQL Warehouse executor
**Description**
- Execute normalized SQL against Databricks SQL Warehouse using JDBC (or DBSQL HTTP client if already in Skadi).

**Acceptance Criteria**
- Given SQL text + bound parameters → returns Arrow stream or row batches to the gateway.
- Supports timeouts and cancellation signals (best effort).

**Tasks**
- Connection pool (config: host/httpPath/token, max pool size).
- Query execution wrapper with:
    - timeout
    - cancellation hook
    - capture DBX query id (for logs)

---

## STORY A4 — SQL dialect bridge (PG/MySQL → Databricks SQL)
**Description**
- Implement a translation layer that converts client SQL to Databricks SQL-friendly form.

**Acceptance Criteria**
- Handles common Tableau-generated patterns encountered in the trace log.
- Supports `LIMIT`, `CAST`, `COALESCE`, `CASE`, aliases, nested selects.
- Parameter markers are preserved and bound correctly.

**Tasks**
- SQL normalizer:
    - stable whitespace/casing normalization for cache keys
- Translation rules:
    - `LIMIT/OFFSET` normalization
    - date/timestamp casts
    - identifier quoting normalization (`"` vs `` ` ``)
- “Compatibility flags” per client (Tableau vs generic JDBC)

---

## STORY A5 — Implement metadata queries (information_schema facade)
**Description**
- Tableau will issue metadata discovery queries. Provide a working metadata surface.

**Acceptance Criteria**
- Tableau can browse:
    - schemas
    - tables/views
    - columns + data types
- Works for at least one MXL catalog/database mapping.

**Tasks**
- Decide mapping:
    - DBX catalog → PG database
    - DBX schema → PG schema
- Implement information schema endpoints or emulate via:
    - intercepting known metadata SQL and answering from DBX system tables/APIs
- Cache metadata responses (short TTL, e.g., 60s–5m)

---

## STORY A6 — Result-set typing + streaming back to client
**Description**
- Map Databricks types to PostgreSQL/MySQL types and stream results.

**Acceptance Criteria**
- Correct handling for:
    - BIGINT/INT
    - DECIMAL(p,s)
    - DOUBLE
    - STRING/VARCHAR
    - DATE/TIMESTAMP (timezone behavior documented)
    - NULLs
- Tableau can render measures/dimensions without type errors.

**Tasks**
- Type mapping table + unit tests.
- Row encoding + chunking.
- Large result protection (server-side limit option in POC, documented).

---

## STORY A7 — Caching layer integration (POC)
**Description**
- Add caching at two levels:
    1) metadata cache
    2) query-result cache for deterministic queries

**Acceptance Criteria**
- Demonstrate warm-cache speed-up on repeated dashboard interactions.
- Cache keys include:
    - normalized SQL
    - parameter values
    - user/role scope (at least per-user or per-credential)
    - (optional) cob-date parameter if present in SQL/filters
- TTL config available.

**Tasks**
- Decide cache policy:
    - conservative exact-match caching for POC
- Add cache hit/miss metrics and log fields:
    - `cache_local` / `cache_s3` (align to your Skadi naming)

---

## STORY A8 — Trace harness for Tableau query patterns
**Description**
- Add logging/tracing to record the exact SQL Tableau sends and the protocol features it triggers.

**Acceptance Criteria**
- One switch enables “Tableau trace mode” that logs:
    - session params
    - prepared statements
    - all SQL text + timing
    - failure reasons + error codes
- Produces a reproducible test corpus.

**Tasks**
- Structured logging fields: `client=tableau`, `session_id`, `query_fingerprint`.
- Store anonymized traces under `testdata/tableau-traces/` (optional).

---

## STORY A9 — POC demo workbook + runbook
**Description**
- Provide a repeatable demo: connect Tableau to Skadi, run 2–3 dashboards, measure.

**Acceptance Criteria**
- `ai/plan/tableau-sql-endpoint.md` includes:
    - setup steps
    - config example
    - how to connect in Tableau
    - expected limitations
    - how to reproduce warm-cache improvements

**Tasks**
- Provide a sample schema mapping for one MXL dataset set.
- Document “known Tableau oddities” encountered.

---

## POC Exit Criteria (Lane A)
- Tableau Desktop connects via Postgres connector
- Dashboard renders successfully
- Warm-cache improvement is observable and recorded (latency + DBX query count/load reduction)
- Known gaps captured as production backlog

---

# Lane B — Production Hardening (Secure, Correct, Operable)

## STORY B1 — Authentication & authorization (enterprise-ready)
**Description**
- Support stronger auth modes and consistent identity mapping.

**Acceptance Criteria**
- Support at least:
    - username/password with secure storage
    - (optional) mTLS
- AuthN is enforced for all sessions.
- AuthZ model defined (even if simple): which schemas/datasets are visible to which principals.

**Tasks**
- Pluggable auth providers interface.
- Credential rotation story.
- Map Skadi principal → DBX access model (service account vs impersonation) documented.

---

## STORY B2 — Robust cancellation, timeouts, and resource controls
**Description**
- Prevent runaways and enable safe multi-user operation.

**Acceptance Criteria**
- Per-session and per-query limits:
    - max rows / max bytes
    - max execution time
    - concurrency caps per user
- Cancellation works (best effort) end-to-end.

**Tasks**
- Query governor.
- Backpressure / streaming controls.

---

## STORY B3 — Protocol completeness for JDBC ecosystem
**Description**
- Expand PG protocol support based on real client behavior.

**Acceptance Criteria**
- Works with:
    - Tableau Desktop
    - at least one generic JDBC tool (DBeaver/DataGrip) using Postgres driver
- Prepared statements + metadata describe calls are stable.

**Tasks**
- Implement additional PG messages as needed.
- Compatibility test suite per client.

---

## STORY B4 — Correctness test suite (golden results)
**Description**
- Lock down behavior as you iterate.

**Acceptance Criteria**
- Automated tests for:
    - type mapping
    - null semantics
    - timezone edge cases
    - decimal scale/rounding
    - ordering/limit correctness
- Golden-result comparisons against direct DBX query output.

**Tasks**
- Create a seeded test dataset in DBX (or local simulation).
- Add integration tests that run in CI (or nightly with secrets).

---

## STORY B5 — Observability (production-grade)
**Description**
- Make it easy to operate and debug.

**Acceptance Criteria**
- Metrics:
    - qps, p50/p95/p99 latency
    - cache hit ratios by cache tier
    - active sessions
    - DBX query duration + failures
- Tracing:
    - correlation IDs spanning client → Skadi → DBX
- Dashboards: basic Grafana/Prometheus templates (or your chosen stack)

**Tasks**
- OpenTelemetry instrumentation (if aligned with Skadi).
- Structured logs with redaction (no tokens/PII).

---

## STORY B6 — Security hardening
**Description**
- Close common attack/abuse paths.

**Acceptance Criteria**
- SQL injection resistant handling (proper binding, no naive string concat).
- Secrets never logged.
- TLS required in production.
- Audit log retained (user → query fingerprint → dataset → cache status).

**Tasks**
- Redaction utilities.
- TLS termination options.
- Audit schema definition.

---

## STORY B7 — Tableau Server/Cloud deployment readiness
**Description**
- Make Skadi usable beyond Desktop tests.

**Acceptance Criteria**
- Document driver requirements and deployment modes:
    - Tableau Server nodes
    - Tableau Bridge (if Cloud)
- Verified connectivity and stability in one non-local environment.

**Tasks**
- Deployment docs + Helm chart updates (if you have k8s).
- Smoke tests in staging.

---

## STORY B8 — MySQL protocol add-on (optional hardening)
**Description**
- Provide MySQL wire protocol as an alternative endpoint using the same execution core.

**Acceptance Criteria**
- MySQL clients can connect and run queries.
- Shared dialect bridge and caching core with PG endpoint.

**Tasks**
- Implement MySQL handshake + query flow.
- Extend type mapping table.
- Add client compatibility tests.

---

# Risks / Watchouts (Call these out early)
- **Tableau metadata storms:** Tableau can issue lots of catalog calls; metadata caching is essential.
- **Timestamp/timezone mismatches:** Decide and document behavior (UTC vs local) and test it.
- **Prepared statement quirks:** Tableau/JDBC drivers vary in how they request metadata for prepared queries; tracing will drive backlog.
- **Databricks SQL dialect variance:** Keep translation rules minimal and trace-driven.

---

# Repo Deliverable
- This file should live at: `ai/plan/tableau-sql-endpoint.md`
- Each story should be created as a GitHub Issue with:
    - Title: `Tableau SQL Endpoint: <story>`
    - Labels: `epic/tableau`, `lane/poc` or `lane/prod`, `area/sql-gateway`, `area/protocol`, `area/cache`, `area/obs`

---

## Suggested GitHub Issue Titles (Copy/Paste)
**POC**
- [POC] SQL Gateway scaffold (`skadi-sql-gateway`)
- [POC] PostgreSQL wire-protocol listener (minimal)
- [POC] Databricks SQL Warehouse executor + pooling
- [POC] SQL dialect bridge (PG→DBX) + normalization
- [POC] information_schema facade + metadata cache
- [POC] Result-set typing + streaming
- [POC] Cache integration for query results (exact match)
- [POC] Tableau trace harness + query corpus
- [POC] Demo workbook + runbook

**Production**
- [PROD] AuthN/AuthZ providers + identity mapping
- [PROD] Query governance: limits, timeouts, cancellation
- [PROD] PG protocol completeness for JDBC ecosystem
- [PROD] Correctness suite: golden results vs DBX
- [PROD] Observability: metrics, tracing, dashboards
- [PROD] Security hardening: TLS, redaction, audit logs
- [PROD] Tableau Server/Cloud deployment readiness
- [PROD] Optional: MySQL wire-protocol endpoint

---

# Demo Runbook (A9)

> **Status:** Lane A complete (A1–A9). This runbook covers local/dev setup.
> For production deployment see Lane B stories (B1–B8).

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Java 21+ | `java -version` to verify |
| Maven 3.9+ | Bundled `./mvnw` wrapper available |
| Databricks workspace | SQL Warehouse (Serverless or Pro); HTTP path + PAT token |
| Tableau Desktop | 2023.1+ recommended; use the built-in **PostgreSQL** connector |
| `psql` (optional) | For smoke-testing the pgwire endpoint before opening Tableau |

---

## 1. Build

```bash
git clone git@github.com:iceforge-io/skadi.git
cd skadi
./mvnw clean package -pl skadi-sql-gateway -am -DskipTests
```

---

## 2. Configure

Edit `skadi-sql-gateway/src/main/resources/application.yml` (or pass as env overrides):

```yaml
skadi:
  sql-gateway:
    pgwire:
      enabled: true
      host: 0.0.0.0
      port: 15432
      auth:
        mode: trust          # trust = any username accepted (dev only)

    metadata:
      enabled: true
      pg-database: postgres  # "Database" field Tableau will show
      dbx-catalog: main      # Databricks Unity Catalog name
      dbx-schema: sales      # Databricks schema to expose

    databricks:
      enabled: true
      host: <workspace>.azuredatabricks.net   # or .databricks.com
      http-path: /sql/1.0/warehouses/<id>
      token: dapi...
      max-pool-size: 5
      query-timeout: 5m

    cache:
      enabled: true
      ttl: 5m
      max-entries: 500

    trace:
      enabled: true
      testdata-path: testdata/tableau-traces  # optional corpus capture
```

---

## 3. Run

```bash
./mvnw -pl skadi-sql-gateway -am spring-boot:run
```

The gateway exposes two endpoints:
- **pgwire** (PostgreSQL protocol): `0.0.0.0:15432`
- **HTTP** (Spring Boot health/metrics): `http://localhost:8090/actuator/health`

---

## 4. Smoke-Test with psql

```bash
psql -h 127.0.0.1 -p 15432 -U demo postgres
```

Expected output:
```
psql (15.x)
Type "help" for help.

postgres=> SELECT 1;
 ?column?
----------
 1
(1 row)

postgres=> SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 5;
 table_name
------------
 orders
 ...
```

---

## 5. Sample Schema Mapping (MXL Dataset)

The metadata facade maps a Databricks Unity Catalog hierarchy to what Tableau sees through the Postgres connector:

| Databricks | Skadi / Tableau |
|---|---|
| Catalog: `main` | Database: `postgres` (Tableau "Database" field) |
| Schema: `sales` | Schema: `public` |
| Table: `orders` | Table: `orders` |
| Table: `customers` | Table: `customers` |

**Sample queries Tableau will generate after connecting:**

```sql
-- Schema discovery
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public';

-- Column discovery
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'orders';

-- Typical dashboard aggregate (after user builds a view)
SELECT region, SUM(revenue) AS total_revenue
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY region
ORDER BY total_revenue DESC
LIMIT 1000;
```

---

## 6. Connect Tableau Desktop

1. Open **Tableau Desktop**
2. **Connect → To a Server → PostgreSQL**
3. Fill in:
   - **Server:** `localhost` (or the host running skadi-sql-gateway)
   - **Port:** `15432`
   - **Database:** `postgres`
   - **Authentication:** Username and Password
   - **Username:** `demo` (any value; ignored in trust mode)
   - **Password:** *(leave blank or enter anything)*
4. Click **Sign In**
5. In the **Data Source** pane: select **Schema: public** → tables appear
6. Drag a table to the canvas to build a worksheet

> **Tableau Server / Bridge:** point at the hostname/IP of the machine running skadi-sql-gateway instead of `localhost`.

---

## 7. Reproducing Warm-Cache Improvements

### Method

1. Ensure `trace.enabled: true` and `testdata-path: testdata/tableau-traces` are set
2. Connect Tableau and run a dashboard with at least one heavy aggregate query
3. Refresh the dashboard a second time (same filters → identical normalized SQL)
4. Compare log output:

```
# First run — cold cache (hits Databricks):
event=query_end session_id=a3f1b2c4e5d6 fingerprint=8c3d1a2f rows=1000 latency_ms=3240 cache_local=MISS cache_s3=SKIP

# Second run — warm cache (served by Skadi, no warehouse query):
event=query_end session_id=a3f1b2c4e5d6 fingerprint=8c3d1a2f rows=1000 latency_ms=3 cache_local=HIT cache_s3=SKIP
```

5. Check **Databricks SQL History** (workspace UI → SQL → Query History) — the second run should produce **no new warehouse query entry**
6. Corpus files at `testdata/tableau-traces/<session-id>.jsonl` contain per-query timing records for offline analysis:

```json
{"ts":"2026-03-14T10:00:01Z","session_id":"a3f1b2c4e5d6","fingerprint":"8c3d1a2f","sql":"SELECT REGION, SUM(REVENUE) AS TOTAL_REVENUE FROM ORDERS ...","rows":1000,"latency_ms":3240,"cache_local":"MISS"}
{"ts":"2026-03-14T10:00:45Z","session_id":"b9e2c3f4a1d5","fingerprint":"8c3d1a2f","sql":"SELECT REGION, SUM(REVENUE) AS TOTAL_REVENUE FROM ORDERS ...","rows":1000,"latency_ms":3,"cache_local":"HIT"}
```

**Expected improvement:** cache HIT latency is typically <10 ms vs 1–30 s for a cold Databricks warehouse query.

---

## 8. Known Limitations (POC)

| Area | Limitation |
|---|---|
| **Result encoding** | Text-only (no binary PG wire format). Types are correct but sent as strings; Tableau parses them client-side. |
| **TLS** | No TLS termination — plaintext only. Do not use over untrusted networks. See B6 for production hardening. |
| **Auth** | Trust mode (any username accepted) or cleartext password. No token/OAuth/mTLS. See B1. |
| **Cache scope** | Local in-memory only. Restarts clear the cache. No S3/distributed tier yet. See B5 / Cache Layer epic. |
| **Cache policy** | Exact-match on normalized SQL + username. Queries with different literal values (e.g. different date filters) are separate cache entries. |
| **Metadata** | Synthetic `information_schema` facade. Schema/table/column lists are statically configured via `dbx-catalog`/`dbx-schema`; not dynamically fetched per-request. |
| **Cancellation** | `CancelRequest` handled (B2 complete): `Statement.cancel()` + Arrow stream abort via cancel flag. Best-effort — driver support varies. |
| **Row limits** | `max-rows` is advisory — set in JDBC `setMaxRows()`, not enforced at SQL level. |
| **Timestamps** | Sent as text using Java `toString()`. UTC assumed. Timezone-aware columns pass through as-is. Test timezone edges before prod. |
| **Concurrency** | Per-user concurrency cap available (B2 complete): set `pgwire.max-concurrent-queries-per-user`. Default: unlimited. |
| **Query timeout** | Per-query timeout available (B2 complete): set `pgwire.query-timeout` (e.g. `5m`). Applied via `setQueryTimeout()` on JDBC path. |
| **Protocol completeness** | Extended query protocol is a minimal subset (Parse/Bind/Describe/Execute/Sync). Edge cases may appear with non-Tableau JDBC clients. See B3. |

---

## 9. Known Tableau Oddities

Patterns observed during A1–A8 implementation that required special handling:

| Behaviour | What Skadi does | Notes |
|---|---|---|
| **SSLRequest on every connect** | Returns `N` (no TLS); Tableau retries with plaintext | Normal Tableau behaviour — not an error |
| **`SET application_name = 'Tableau x.y'`** | Accepted as no-op; value captured for `client=tableau` detection in trace logs | Tableau sends this immediately after auth |
| **`SHOW standard_conforming_strings`** | Returns `on` | JDBC bootstrap; must respond or driver hangs |
| **`SELECT current_setting('...')`** | Returns empty string | Driver introspection call |
| **`SELECT version()`** | Returns `"Skadi SQL Gateway (pgwire)"` | Tableau accepts any non-empty string |
| **Extended query for all data queries** | Parse → Bind → Describe → Execute → Sync | Tableau never uses Simple Query (`Q`) for data; only for metadata probes |
| **`information_schema` storms on connect** | Absorbed by metadata cache (TTL 2m) | Can be 20–50 queries on first connect; cache prevents repeated Databricks roundtrips |
| **`RESET` commands on disconnect** | Accepted as no-op | Tableau tears down cleanly |
| **`SELECT 1` keepalives** | Handled inline; never reaches Databricks | Tableau uses this to test connection liveness |
| **Prepared statement per dashboard sheet** | One Parse per query per sheet | A 5-sheet dashboard issues 5 Parse messages before any Execute |
| **Describe before Bind** | Skadi sends a generic `RowDescription` with `TEXT` OID | Tableau uses describe to pre-allocate columns; `TEXT` type is safe for POC |

---

## 🔮 Future Direction (Out of Scope)

A longer-term evolution where Skadi becomes a **native or hybrid SQL execution engine** is documented under:

```
ai/future-architecture-options/skadi-warehouse-lite/
```

This is explicitly **out of scope** for this plan.

This plan focuses on:

* Databricks-backed execution
* SQL compatibility (PgWire/JDBC)
* Caching and acceleration
* Production hardening

---
