# Skadi — Development Status

> Updated: 2026-05-17
> Branch: `feature/114-semantic-execution-resilience-integration-tests`
> Last commit: pending — Lane F F3: semantic execution resilience integration tests — skadi#114
> Build: ✅ 787 tests passing (skadi-semantic: 556, skadi-server: 231; gateway unchanged)

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
| B8 | MySQL wire-protocol endpoint (optional) | ✅ Done | pending (PR #36) | MySqlWireServer, COM_QUERY, mysql_native_password auth; 31 new tests |

---

## B8 Implementation Summary

**Issue:** #30 (to be closed)

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

### Tests added (31 total)
- `MySqlHandshakeTest` (4): greeting packet structure, unique connection IDs, trust auth, password rejection
- `MySqlNativePasswordTest` (5): correct password verifies, wrong fails, empty/null token, different challenges
- `MySqlQueryRoutingTest` (6): SELECT 1, multi-row result, row value encoding, COM_PING, metadata facade, unsupported command ERR
- `MySqlTypeMapperTest` (14): JDBC → MySQL type codes, flags, display lengths
- `MySqlWireHealthIndicatorTest` (2): UP when running, DOWN when disabled

---

## B7 Implementation Summary

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
| L10 | Medium | Gateway embeds own JDBC pool + cache instead of delegating to `skadi-server` — duplicates execution concerns; `SkadiServerQueryExecutionService` activated in Lane E (DQR-002 resolved); full SQL gateway convergence remains deferred — see DQR-004 |
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

## Lane D — Contract Loading, Resolution, and Runtime Activation (Complete ✅)

| Story | Description | Status | Commit | Issue |
|---|---|---|---|---|
| D1 | Lane D activation boundary document | ✅ | `4606eff` | #62 |
| D2 | Contract definition format decision (JSON canonical) | ✅ | `bc081e8` | #63 |
| D3 | File-backed JSON contract loader | ✅ | `4360f96` | #64 |
| D6 | Contract validation and diagnostics | ✅ | `3339075` | #67 |
| D4 | Runtime ContractRegistry population | ✅ | `97eea43` | #65 |
| D5 | Semantic contract resolution service | ✅ | `2f543ab` | #66 |
| D7 | Read-only contract metadata endpoint | ✅ | `c0506a8` | #68 |
| D8 | Dev-status and Lane D runbook | ✅ | — | #69 |

All D-lane issues closed. Committed directly to `main`.

---

## Lane D Completed — Summary

**Lane D scope:** contract loading, validation, registry population, resolution, and read-only metadata surface — no semantic planner, no SQL generation, no execution path activation.

### What was built in Lane D

| Capability | Key Components | Module |
|---|---|---|
| Contract format decision | ADR-011 (JSON canonical); DQR-001 resolved | `ai/adr/` |
| JSON contract loader | `ContractLoader` interface, `JsonContractLoader` (Jackson), `ContractLoadException` | `skadi-semantic` |
| Contract validation | `ContractValidator`, `SemanticContractValidator`, `ContractValidationResult`, `ContractValidationIssue`, `ContractValidationSeverity` (8 issue codes) | `skadi-semantic` |
| Registry population | `ContractRegistryPopulator`, `ContractRegistryPopulationResult`, `ContractRegistryPopulationException`, `LoadedContractRegistry` (read-only) | `skadi-semantic` |
| Contract resolution | `RegistrySemanticContractResolver` (no access policy enforcement) | `skadi-semantic` |
| Metadata endpoint | `SemanticContractMetadataController` (`GET /api/semantic/contracts`, `/validation`, `/{name}`), `SemanticContractConfiguration`, `SemanticContractProperties` | `skadi-server` |
| Format docs | `ai/lane-d/contract-format.md`, ADR-011, DQR-001 (resolved) | `ai/` |
| Activation boundary | `ai/lane-d/lane-d-activation-boundary.md` | `ai/` |
| Runbook | `ai/lane-d/lane-d-runbook.md` | `ai/` |

### Key design decisions

| Decision | Outcome |
|---|---|
| Contract file format | JSON (ADR-011 Accepted); YAML deferred to post-Lane D |
| Validation pipeline | `ContractLoader` → `SemanticContractValidator` → `ContractRegistryPopulator` |
| ERROR issues | Abort registry population; `registry=null` in result |
| WARNING issues | Allow registry population; warnings available in result |
| Registry mutability | `LoadedContractRegistry` is read-only after construction |
| Access policy | Not enforced in Lane D; all principals see all contracts |
| Endpoint default | `skadi.semantic.contracts.enabled=false`; server startup never blocked |
| `SkadiServerQueryExecutionService` | Skeleton unchanged; HTTP activation deferred to DQR-002 |

### Endpoint summary

| Method | Path | Behavior |
|---|---|---|
| `GET` | `/api/semantic/contracts` | List all loaded contracts (summary); empty when disabled |
| `GET` | `/api/semantic/contracts/validation` | Validation status of loaded set (valid/errors/warnings/issues) |
| `GET` | `/api/semantic/contracts/{name}` | Full contract detail; `404` if not found |

### Test count progression

| Milestone | skadi-semantic | skadi-server | Total |
|---|---|---|---|
| C8 complete (baseline) | 258 | 101 | 359 |
| D3 complete | 282 | 101 | 383 |
| D6 complete | 320 | 101 | 421 |
| D4 complete | 354 | 101 | 455 |
| D5 complete | 378 | 101 | 479 |
| D7 complete | 378 | 116 | 494 |

### Lane D non-goals (enforced throughout)

- No semantic query planner or optimizer
- No SQL generation from contract metadata
- No semantic rule execution engine
- No entitlement enforcement engine
- No `SkadiServerQueryExecutionService` activation (DQR-002 still open)
- No cache behavior changes
- No Databricks semantic execution path
- No UI runtime or React components
- No AI chatbot or LLM integration
- No lineage database integration
- No `skadi-sql-gateway` changes

---

## Lane E1 — Semantic Metadata Foundation (Complete ✅)

Stories #72–#82 (EPIC #82). Contract explanation metadata, ADR-012 fitness tests,
and screen-context model — all committed to `main` as part of PR #94 (`ca87c72`).

| Story | Description | Status | Issue |
|---|---|---|---|
| E1.1–E1.4 | Contract explanation metadata records | ✅ | #72–#75 |
| E1.6 | Metadata API endpoints | ✅ | — |
| E1.8 | Request validation endpoint | ✅ | — |
| E1.9 | Screen-context and widget-binding model | ✅ | #80 |
| E1.10 | ADR-012 fitness tests and guardrails | ✅ | #81 |

Key model types (all in `org.iceforge.skadi.semantic.screen`):
`DashboardScreenContext`, `WidgetSemanticBinding`, `BoundMeasure`, `BoundDimension`,
`AppliedFilter`, `ContractBinding`, `UserEntitlementScope`, `ActiveWidgetContext`,
`ExplainWidgetBindingRequest` — serializable model records only; no runtime, no LLM,
no query execution, no SQL generation, `skadi-sql-gateway` untouched.

ADR-012 guardrails verified: enriched metadata load/registry roundtrip (21 tests);
validation endpoint rejects unknown contract/measure/dimension, non-groupable grouping,
and non-filterable filter; structural test confirms validation endpoint has no execution
dependency.

Tests: `ScreenContextModelTest` (35), `Adr012FitnessTest` (21), `SemanticRequestValidationControllerTest` (21).

---

## Lane F — Semantic Execution Resilience and Operational Hardening

| Story | Description | Status | Issue |
|---|---|---|---|
| F1 | Semantic execution health, readiness, and circuit-breaker behavior | ✅ | #110 |
| F2 | Semantic execution readiness endpoint and operator runbook | ✅ | #112 |
| F3 | Semantic execution resilience integration tests | ✅ | #114 |

**Epic:** #109 — harden the Lane E semantic execution delegation path before buddy-chat, dashboard explanation, or gateway convergence depend on it.

---

## Lane F Completed — F3 Summary

**Issue:** #114 | **Branch:** `feature/114-semantic-execution-resilience-integration-tests`

**Lane F F3 scope:** Integration tests proving resilience, readiness, and diagnostics behavior through property binding, bean creation, and controller-level MockMvc assertions.

### What was built in F3

| Capability | Coverage | Module |
|---|---|---|
| Property binding | `Binder`-based tests verifying `skadi.semantic.execution.circuit-breaker.*` YAML binding (defaults, custom values, disabled flag) | `skadi-server` |
| Bean creation | `SemanticContractConfiguration.semanticExecutionCircuitBreaker()` creates CB with correct threshold, URL, and disabled state | `skadi-server` |
| Failure states in diagnostics | UNAVAILABLE, TIMEOUT, FAILED, CIRCUIT_OPEN → correct `readiness` field in response | `skadi-server` |
| Full lifecycle | Failures → CIRCUIT_OPEN → clock-advanced probe → HEALTHY/READY recovery; probe failure re-opens | `skadi-server` |
| Failure count | Increments correctly in diagnostics response across multiple `recordFailure` calls | `skadi-server` |
| Secret safety | No `password`, `jdbcPassword`, `secret`, `token`, `credentials`, `jdbcUrl`, `username` in response | `skadi-server` |

### Test count progression

| Milestone | skadi-semantic | skadi-server | Total |
|---|---|---|---|
| F2 complete (baseline) | 556 | 212 | 768 |
| F3 complete | 556 | 231 | 787 |

### Lane F F3 non-goals (enforced throughout)

- No `skadi-sql-gateway` changes
- No real Databricks or network calls
- No new API surface
- No LLM integration, UI runtime, or SQL generation

---

## Lane F Completed — F2 Summary

**Issue:** #112 | **Branch:** `feature/112-semantic-execution-readiness-runbook`

**Lane F F2 scope:** Operational readiness classification and operator runbook for the semantic execution delegation path.

### What was built in F2

| Capability | Key Components | Module |
|---|---|---|
| Readiness enum | `SemanticExecutionReadiness` — `READY, DISABLED, DEGRADED, UNAVAILABLE`; `from(SemanticExecutionHealthStatus)` mapping | `skadi-semantic` |
| Diagnostics field | `health.readiness` added to `GET /api/semantic/v1/execution/status` response | `skadi-server` |
| Readiness tests | `SemanticExecutionReadinessTest` — 7 tests covering all 6 health-to-readiness mappings | `skadi-semantic` |
| Operator runbook | `ai/lane-f/semantic-execution-readiness-runbook.md` — status interpretation, failure modes, config reference, deployment gate pattern | `ai/` |

### Test count progression

| Milestone | skadi-semantic | skadi-server | Total |
|---|---|---|---|
| F1 complete (baseline) | 549 | 211 | 760 |
| F2 complete | 556 | 212 | 768 |

### Lane F F2 non-goals (enforced throughout)

- No `skadi-sql-gateway` changes
- No LLM integration, UI runtime, or SQL generation
- No full platform health framework
- No secrets exposed in diagnostics

---

## Lane F Completed — F1 Summary

**Issue:** #110 | **Branch:** `feature/110-semantic-execution-resilience`

**Lane F F1 scope:** Circuit-breaker protection for the semantic execution delegation path. `skadi-sql-gateway` intentionally untouched.

### What was built in F1

| Capability | Key Components | Module |
|---|---|---|
| Health status enum | `SemanticExecutionHealthStatus` — `DISABLED, HEALTHY, UNAVAILABLE, TIMEOUT, FAILED, CIRCUIT_OPEN` | `skadi-semantic` |
| Health snapshot | `SemanticExecutionHealthSnapshot` — immutable record; `@JsonInclude(NON_NULL)` | `skadi-semantic` |
| Circuit breaker | `SemanticExecutionCircuitBreaker` — thread-safe (`synchronized`), injectable `Clock`; half-open probe; `alwaysAllow` and `disabled` factories | `skadi-semantic` |
| CB configuration | `SemanticExecutionProperties.CircuitBreakerProperties` — `enabled`, `failureThreshold`, `openDurationMs` | `skadi-server` |
| Bean wiring | `semanticExecutionCircuitBreaker` bean in `SemanticContractConfiguration`; `queryExecutionService` takes CB as 5th param | `skadi-server` |
| Diagnostics endpoint | `GET /api/semantic/v1/execution/status` — `health` block added: `status, failureCount, failureThreshold, lastSuccessAt, lastFailureAt, lastFailureReason, circuitOpenUntil` | `skadi-server` |
| Tests | `SemanticExecutionCircuitBreakerTest` (22 tests); 6 new diagnostics controller tests for health snapshot and disabled/active states | `skadi-semantic`, `skadi-server` |

### Test count progression

| Milestone | skadi-semantic | skadi-server | Total |
|---|---|---|---|
| E2 complete (baseline) | 529 | 203 | 732 |
| F1 complete | 549 | 211 | 760 |

### Lane F non-goals (enforced throughout)

- No `skadi-sql-gateway` changes
- No semantic contract behavior changes
- No LLM integration, UI runtime, or SQL generation
- No execution abstraction redesign

---

## Lane E — Semantic Execution Activation (Complete ✅)

| Story | Description | Status | Commit | Issue |
|---|---|---|---|---|
| E1 | Semantic execution activation (`SkadiServerQueryExecutionService`) | ✅ | `42e4a4a` | #97 |
| E2 | Semantic execution observability and diagnostics | ✅ | `03ecafc` | #104 |

All Lane E stories closed. PRs merged to `main`.

---

## Lane E Completed — E2 Summary

**Issue:** #104 | **PR:** #105 | **Commit:** `03ecafc`

**Lane E E2 scope:** Add structured logging, in-memory metrics counters, and a diagnostics endpoint for the Lane E semantic execution path. `skadi-sql-gateway` intentionally untouched.

### What was built in E2

| Capability | Key Components | Module |
|---|---|---|
| Metrics interface | `SemanticExecutionMetrics` — 7 outcome methods; exactly one terminal counter per `execute()` call | `skadi-semantic` |
| No-op default | `NoOpSemanticExecutionMetrics` — singleton, used when no recording is needed | `skadi-semantic` |
| Structured logging | SLF4J at DEBUG/INFO/WARN/ERROR on every execution branch in `SkadiServerQueryExecutionService` | `skadi-semantic` |
| Counter registry | `SemanticExecutionMetricsRegistry` — `@Component`, thread-safe `LongAdder` counters | `skadi-server` |
| Diagnostics endpoint | `GET /api/semantic/v1/execution/status` — returns `active`, `serverUrl`, `datasourceId`, lifetime counter snapshot | `skadi-server` |
| Tests | `SemanticExecutionMetricsTest` (5), `SemanticExecutionMetricsRegistryTest` (9), `SemanticExecutionDiagnosticsControllerTest` (8) | both |

### Test count progression (Lane E complete)

| Milestone | skadi-semantic | skadi-server | Total |
|---|---|---|---|
| D7 complete (baseline) | 378 | 116 | 494 |
| E1 complete | 524 | 186 | 710 |
| E2 complete | 529 | 203 | 732 |

---

## Lane E Completed — E1 Summary

**Issue:** #97 | **PR:** #102 | **Commit:** `42e4a4a`

**Lane E E1 scope:** Activate the semantic execution path so semantic requests delegate to `skadi-server` via HTTP. `skadi-sql-gateway` intentionally untouched — SQL gateway convergence is a separate future question tracked in DQR-004.

### What was built in E1

| Capability | Key Components | Module |
|---|---|---|
| HTTP delegation | `SkadiServerQueryExecutionService` — replaces skeleton; POSTs to `POST /api/v1/queries`; handles cache-hit (200 SUCCEEDED) and async (202 ACCEPTED + status poll) paths | `skadi-semantic` |
| Execution config | `SemanticExecutionProperties` (`skadi.semantic.execution.server-url`, `datasource-id`) | `skadi-server` |
| Bean wiring | `queryExecutionService` bean added to `SemanticContractConfiguration` | `skadi-server` |
| Tests | `SemanticExecutionActivationTest` (5 tests) + updated `ServiceBoundaryTest` (4 delegation tests) + updated `ContractCompositionTest` | `skadi-server`, `skadi-semantic` |

### Key design decisions

| Decision | Outcome |
|---|---|
| Execution delegation topology | DQR-002 resolved: Option 4 (partial convergence) — semantic path delegates to `skadi-server`; SQL gateway direct path unchanged |
| SQL gateway convergence | Explicitly deferred; tracked as future scope in DQR-004 |
| `skadi-sql-gateway` | **Untouched** — no changes to gateway module in Lane E |
| Test approach | Hand-rolled JDK `HttpServer` fakes — no Mockito, no Databricks, no Spring context |

### Lane E non-goals (enforced throughout)

- No `skadi-sql-gateway` changes
- No SQL gateway traffic routed through `skadi-server`
- No convergence of gateway and server execution ownership
- No pgwire/mysql semantic awareness
- No full gateway convergence (deferred to DQR-004)

---

## Architecture Evolution (Lane C+/D+/E+)

Architecture docs, ADRs, and DQRs:

- `ai/architecture-evolution.md` — full evolution proposal, roadmap, reusable component analysis
- `ai/adr/ADR-004-semantic-query-layer.md`
- `ai/adr/ADR-005-semantic-contracts.md` (Proposed — superseded for format question by ADR-011)
- `ai/adr/ADR-006-dashboard-brick-model.md`
- `ai/adr/ADR-007-ai-chat-integration.md`
- `ai/adr/ADR-008-lane-c-scope.md` — Lane C scope decision (Accepted)
- `ai/adr/ADR-009-contracts-before-planning.md` — semantic contracts before planner (Accepted)
- `ai/adr/ADR-010-cache-positioning.md` — cache stays below all consumers (Accepted)
- `ai/adr/ADR-011-contract-definition-format-json-canonical.md` — JSON canonical for Lane D (Accepted)
- `ai/adr/ADR-012-buddy-chat-semantic-model-interrogation.md` — buddy chat must interrogate semantic model for query execution and explanation; must not maintain independent business definitions (Accepted)
- `ai/dqr/DQR-001-contract-definition-format.md` — **Resolved** (JSON canonical; YAML deferred)
- `ai/dqr/DQR-002-semantic-execution-delegation.md` — **Resolved** (Option 4 — partial convergence; issue #97)
- `ai/dqr/DQR-003-lineage-market-risk-brain-seams.md` — lineage/MRB integration seams (Open)
- `ai/dqr/DQR-004-sql-gateway-convergence.md` — full SQL gateway / semantic execution convergence (Open; future scope)

Lane D is complete. Contracts are loadable, validateable, resolvable, and inspectable via a read-only HTTP endpoint. Lane E E1 activated `SkadiServerQueryExecutionService` (DQR-002 resolved); Lane E E2 added structured logging, `SemanticExecutionMetrics` counters, and `GET /api/semantic/v1/execution/status` diagnostics. `skadi-sql-gateway` was untouched throughout Lane E. SQL gateway convergence remains a future design question; see DQR-004.

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
