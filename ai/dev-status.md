# Skadi — Development Status

> Updated: 2026-05-14
> Branch: `main`
> Last commit: see Lane C section below
> Build: ✅ 258 tests passing (`mvn verify -pl skadi-semantic -am`; gateway/server test counts unchanged)

---

## Lane A — POC (Complete ✅)

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

## Lane B — Production Hardening (Complete ✅)

| Story | Description | Status | Commit | Issue | PR |
|---|---|---|---|---|---|
| B1 | Auth & authorisation (enterprise-ready) | ✅ | `adda9fd` | #23 | — |
| B2 | Cancellation, timeouts, resource controls | ✅ | `5c07c1b` | #24 | — |
| B3 | Protocol completeness for JDBC ecosystem | ✅ | `0243425` | #25 | #31 |
| B4 | Correctness test suite (golden results) | ✅ | `ce47076` | #26 | #32 |
| B5 | Observability — production-grade | ✅ | `d2c7f7a` | #27 | #33 |
| B6 | Security hardening — TLS, redaction, audit log | ✅ | `287740e` | #28 | #34 |
| B7 | Tableau Server / Cloud deployment readiness | ✅ | `4cb1194` | #29 | #35 |
| B8 | MySQL wire-protocol endpoint (optional) | ✅ | `3d46db7` | #30 | #36 |

All B-lane issues closed. All PRs merged to `main`.

---

## Lane B Completed — Summary

**EPIC #10 (Tableau SQL Endpoint) closed 2026-05-13.**

### What was built across Lanes A + B

| Capability | Key Components |
|---|---|
| PostgreSQL wire protocol | `PgWireServer`, `PgWireSession`, full extended-query protocol |
| MySQL wire protocol | `MySqlWireServer`, `MySqlWireSession`, Handshake v10, `mysql_native_password` |
| SQL dialect bridge | `SqlDialectBridge`, `SqlNormalizer`, PG/MySQL → Databricks SQL |
| Auth (3 modes) | `AllowAllAuthProvider`, `PlaintextAuthProvider`, `BcryptAuthProvider` |
| Per-user ACL | `PrincipalPolicyRegistry` — schema-level access control |
| Query governance | `QueryGovernor` — concurrency cap + timeout + cancellation |
| In-memory cache | Arrow IPC + text-row TTL cache; two execution paths |
| Metadata facade | Synthetic `information_schema` + `pg_catalog` covering 15+ query patterns |
| Observability | Micrometer/Prometheus metrics: QPS, p50/p95/p99 latency, cache tiers, sessions |
| Correlation IDs | `query_id` MDC + Databricks `ApplicationName` forwarding |
| Security hardening | `SecretRedactor`, `SqlSecurityValidator`, `AuditLog`, require-ssl enforcement |
| Deployment | Dockerfile, docker-compose, k8s manifest, smoke-test script |
| Health indicators | `PgWireHealthIndicator`, `MySqlWireHealthIndicator` |
| Test suite | 232 unit + integration tests; H2-backed correctness harness |

### Test count by lane

| Milestone | Tests | Added |
|---|---|---|
| B3 complete | ~130 | +13 |
| B4 complete | 154 | +36 |
| B5 complete | 167 | +13 |
| B6 complete | 199 | +32 |
| B7 complete | 201 | +2 |
| B8 complete | 232 | +31 |

---

## Technical Debt Register

These items were discovered during Lane B and are deferred — they do not block Lane C.
Items prefixed `[critical]` should be addressed before production traffic.

### Protocol

| ID | Severity | Detail |
|---|---|---|
| L14 | Medium | `Execute` sends `RowDescription` even when `Describe` already did — DBeaver/DataGrip see duplicate `T` messages; Tableau unaffected |
| L15 | Low | Binary-format bind params (`fmt=1`) decoded as UTF-8 text with a warning; correct decoding requires type OID dispatch |
| L18 | Low | `BOOLEAN` renders as `"true"`/`"false"`; PostgreSQL native uses `"t"`/`"f"`; most clients accept both |

### Cache

| ID | Severity | Detail |
|---|---|---|
| L2 | Medium | Dialect bridge bypassed on path 1 zero-param queries — raw PG SQL reaches Databricks unchanged |
| L3 | Medium | Dataset version absent from cache keys — stale results possible if upstream data refreshes within TTL |
| L5 | Low | Cache config split: `SqlGatewayProperties.Cache` and `CacheProperties` bind same YAML prefix; different fields silently ignored |
| L6 | Low | Static cache singletons (`QUERY_CACHE`, `METADATA_CACHE`) ignore Spring-managed config for `maxEntries` and `Metadata.ttl` |

### Security

| ID | Severity | Detail |
|---|---|---|
| L9 / L22 | **[critical]** | STARTTLS upgrade not implemented — `SSLRequest` still returns `N` even when `tls.enabled=true`; use stunnel/Envoy for production TLS until resolved |
| L23 | Low | Schema ACL denials not audited — `applyPolicyFilter()` silently removes rows; no `audit_acl_deny` event |
| L24 | Low | `keystorePassword` stored in plaintext in config — use env-var injection for production |

### Architecture

| ID | Severity | Detail |
|---|---|---|
| L10 | Medium | Gateway embeds own JDBC pool + cache instead of delegating to `skadi-server` — duplicates execution concerns; `SkadiServerQueryExecutionService` skeleton created in Lane C; HTTP activation deferred pending DQR-002 resolution |
| L11 | Low | `JdbcArrowStreamer` duplicated in `skadi-core` and `skadi-server`; copies may diverge |
| L13 | Low | `ai/query-flow.md` referenced in `ai/claude-instructions.md` but does not exist |

### Observability

| ID | Severity | Detail |
|---|---|---|
| L19 | Low | No Grafana dashboard template — metrics are Prometheus-ready; dashboard JSON not yet provided |
| L20 | Low | `QueryCacheMetrics` path-1 static singleton not bridged to Micrometer — `cache_tier` tags on `skadi_queries` provide equivalent visibility |
| L21 | Low | No OpenTelemetry tracing spans — `query_id` MDC + Databricks `ApplicationName` covers correlation for now |

### Testing

| ID | Severity | Detail |
|---|---|---|
| L16 | Low | `SqlGatewayIT` placeholder — real Databricks integration tests need CI secrets + seeded warehouse |
| L17 | Medium | `TIMESTAMPTZ` Arrow path (`TimeStampMilliTZVector`) not exercised by H2; Databricks `timestamp_ltz` columns untested |

---

## Lane C — Contracts, Skeletons, and Boundaries (Complete ✅)

| Story | Description | Status | Commit | Issue |
|---|---|---|---|---|
| C1 | Platform boundary model (architecture doc) | ✅ | `61aa0a3` | #39 |
| C6 | ADRs 008–010 and DQRs 001–003 | ✅ | `3e6c76e` + `0e2c4aa` | #44 |
| C2.1 | `skadi-semantic` Maven module + package scaffold | ✅ | `b32d59f` | #55 |
| C2.2 | Core semantic contract records | ✅ | `dc02035` | #49 |
| C2.3 | Access policy and cache policy skeletons | ✅ | `8a1833c` | #50 |
| C2.4 | ContractRegistry interface | ✅ | `51a21fe` | #51 |
| C2.5 | JSON serialization fixtures and tests | ✅ | `95bf0f7` | #52 |
| C2.6 | C2 documentation and implementation notes | ✅ | `5f45635` | #53 |
| C3 | Query contract and output-shape metadata | ✅ | `7cec95e` | #41 |
| C4 | Cache boundary contracts | ✅ | `e2377de` | #42 |
| C5 | Service interfaces for semantic-aware execution | ✅ | `d462813` | #43 |
| C7 | Cross-lane contract composition tests | ✅ | `d41868b` | #45 |
| C8 | Dev-status and Lane C runbook | ✅ | — | #46 |

All C-lane issues closed. No PRs — committed directly to `main`.

---

## Lane C Completed — Summary

**Lane C scope:** contracts, skeletons, and boundaries only — no production behavior change.

### What was built in Lane C

| Capability | Key Components |
|---|---|
| Module scaffold | `skadi-semantic` Maven module; 5 packages; inherits from `skadi-parent` |
| Semantic contract model | `SemanticContract`, `SemanticEntity`, `SemanticEndpoint`, `SemanticMeasure`, `SemanticDimension` |
| Policy records | `SemanticAccessPolicy` (principal allow-lists), `SemanticCachePolicy` (TTL/NONE strategy) |
| Contract registry | `ContractRegistry` interface; `InMemoryContractRegistry` (test-scope only) |
| Query contract + shape | `SemanticQueryContract`, `SemanticOutputShape`, `SemanticOutputColumn`, `SemanticReference` |
| Cache boundary | `CacheIdentity` (SHA-256 fingerprint), `CacheContract`, `CacheLookupRequest`, `CacheWriteRequest`, `CacheLookupResult`, `CacheWriteResult` |
| Service interfaces | `QueryExecutionService`, `QueryMetadataService`, `SemanticResolutionService`, `CacheLookupService`, `LineageContextProvider` |
| No-op implementations | `NoOpCacheLookupService`, `NoOpLineageContextProvider` (always-absent / always-empty) |
| Skeleton | `SkadiServerQueryExecutionService` (throws `UnsupportedOperationException`; activation deferred to DQR-002) |
| Request/result types | `ExecutionContext`, `QueryExecutionRequest`, `QueryExecutionResult`, `QueryMetadataRequest`, `QueryMetadataResult`, `SemanticResolutionRequest`, `SemanticResolutionResult` |
| JSON test fixtures | `sample-contract.json`, `sample-query-contract.json`, `sample-cache-boundary.json` |
| Architecture docs | `ai/architecture/platform-boundary-model.md`, ADR-008, ADR-009, ADR-010, DQR-001, DQR-002, DQR-003 |
| Runbook | `ai/lane-c/lane-c-runbook.md` |

### Test count by lane

| Milestone | Tests | Added |
|---|---|---|
| B8 complete | 232 | — |
| C2.5 complete | 245 | +13 |
| C3 complete | 249 | +4 |
| C4 complete | 254 | +5 |
| C5 complete | 258 | +4 |
| C7 complete | 258 | 0 (composition tests count toward C5 total) |

### Lane C non-goals (enforced throughout)

- No Spring beans in `skadi-semantic`
- No SQL generation or dialect translation
- No Databricks, S3, or REST calls
- No YAML loading, JSON Schema validation, or UI runtime
- No production code changes in `skadi-sql-gateway` or `skadi-server`

---

## Architecture Evolution (Lane C+)

Architecture docs and ADRs for the next phase are in:

- `ai/architecture-evolution.md` — full evolution proposal, roadmap, reusable component analysis
- `ai/adr/ADR-004-semantic-query-layer.md`
- `ai/adr/ADR-005-semantic-contracts.md`
- `ai/adr/ADR-006-dashboard-brick-model.md`
- `ai/adr/ADR-007-ai-chat-integration.md`
- `ai/adr/ADR-008-lane-c-scope.md` — Lane C scope decision (Accepted)
- `ai/adr/ADR-009-contracts-before-planning.md` — semantic contracts before planner (Accepted)
- `ai/adr/ADR-010-cache-positioning.md` — cache stays below all consumers (Accepted)
- `ai/dqr/DQR-001-contract-definition-format.md` — contract format: YAML vs JSON (Open)
- `ai/dqr/DQR-002-semantic-execution-delegation.md` — execution topology (Open)
- `ai/dqr/DQR-003-lineage-market-risk-brain-seams.md` — lineage/MRB integration seams (Open)

Lane C is complete. The `skadi-semantic` module contains contracts, interfaces, and skeletons only. No production execution is wired. See `ai/lane-c/lane-c-runbook.md` for activation guidance.

---

## B8 Implementation Detail

**Issue:** #30 (closed 2026-05-13) | **PR:** #36 (merged 2026-05-13) | **Commit:** `3d46db7`

### New package: `mysqwire`
- `MySqlWireServer` — TCP server; accepts connections, spawns `MySqlWireSession` per client
- `MySqlWireSession` — MySQL Handshake v10, `mysql_native_password` challenge-response auth, COM_QUERY text protocol, COM_PING, COM_QUIT, COM_INIT_DB
- `MySqlWireServerLifecycle` — `SmartLifecycle` Spring component; mirrors `PgWireServerLifecycle`
- `MySqlWireHealthIndicator` — `@Component("mySqlWire")` for `/actuator/health/mySqlWire`; gated by `skadi.sql-gateway.mysqlwire.enabled=true`
- `MySqlExecutorProviderHolder` / `MySqlExecutorProviderWiring` — static bridge for shared Databricks DataSource
- `JdbcToMySqlTypeMapper` — JDBC type → MySQL column type codes, flags, display widths

### Protocol scope
- Handshake v10: server greeting with random 20-byte challenge, capability flags, auth plugin name
- `mysql_native_password`: SHA1(password) XOR SHA1(challenge + SHA1(SHA1(password))); verified server-side from plaintext or trust-mode
- COM_QUERY: SQL routed through `SqlDialectBridge` (MYSQL dialect) → metadata facade → JDBC executor → MySQL result set (column count, column def ×N, EOF, row data ×N, EOF)
- Trust mode: accepts any username/password; password mode: validates challenge-response

### Config
- `SqlGatewayProperties.MySqlWire` record added (7th field in `SqlGatewayProperties`)
- `SqlDialectBridgeOptions.defaultForMySqlWire()` factory added
- `application.yml`: `skadi.sql-gateway.mysqlwire.enabled=false` (port 13306)

### Tests added (31)
- `MySqlHandshakeTest` (4): greeting packet structure, unique connection IDs, trust auth, password rejection
- `MySqlNativePasswordTest` (5): correct password verifies, wrong fails, empty/null token, different challenges
- `MySqlQueryRoutingTest` (6): SELECT 1, multi-row result, row value encoding, COM_PING, metadata facade, unsupported command ERR
- `MySqlTypeMapperTest` (14): JDBC → MySQL type codes, flags, display lengths
- `MySqlWireHealthIndicatorTest` (2): UP when running, DOWN when disabled

---

## B7 Implementation Detail

**Issue:** #29 (closed 2026-05-13) | **PR:** #35 (merged 2026-05-13) | **Commit:** `4cb1194`

### PgWireHealthIndicator
- `@Component("pgWire")` — `/actuator/health/pgWire`; reports `UP` with port detail when bound; `DOWN` when disabled
- 2 unit tests: UP path (real server on ephemeral port), DOWN path (disabled lifecycle)

### Docker packaging
- `skadi-sql-gateway/Dockerfile`: multi-stage build (eclipse-temurin:21-jdk-jammy → 21-jre-jammy); non-root `skadi` user
- `skadi-sql-gateway/docker-compose.yml`: full env-var mapping; Prometheus/Grafana commented

### Deployment documentation
- `docs/deployment/README.md`, `docker.md`, `tableau-server.md`, `tableau-bridge.md`, `production-checklist.md`, `troubleshooting.md`
- k8s manifest with readiness probe on `/actuator/health/pgWire`

### Smoke test + README
- `scripts/smoke-test.sh`: HTTP health, pgwire connectivity, metadata facade, optional Databricks query; exit 0/1
- `skadi-sql-gateway/README.md`: rewritten — quick start, ports, auth modes, config reference, health endpoints

---

## B2–B6 Implementation Detail

See git history for full implementation details. Key commits:

| Story | Commit | PR | Summary |
|---|---|---|---|
| B6 | `287740e` | #34 | `SecretRedactor`, `SqlSecurityValidator`, `AuditLog`, require-ssl, 32 tests |
| B5 | `d2c7f7a` | #33 | Micrometer metrics, `GatewayMetrics`, correlation IDs, 13 tests |
| B4 | `ce47076` | #32 | Arrow/timestamp bug fixes, 36 golden-result tests, `GatewayCorrectnessHarness` |
| B3 | `0243425` | #31 | Extended-query Bind params, `ArrowIpcRowWriter`, `SessionRegistry`, dead code removed |
| B2 | `5c07c1b` | — | `QueryGovernor`, query timeout, `CancelRequest` → `Statement.cancel()` + Arrow cancel flag |
| B1 | `adda9fd` | — | `AuthProvider` SPI, bcrypt store, `PrincipalPolicyRegistry` schema ACL |
