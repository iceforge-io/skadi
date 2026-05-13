# Skadi — Architecture Evolution: Lane C and Beyond

> Date: 2026-05-13
> Status: Proposed
> Follows: Completion of EPIC #10 (Tableau SQL Endpoint, Lanes A + B)

---

## 1. Current Platform Capabilities (Baseline)

After Lanes A and B, Skadi is a production-hardened SQL gateway with the following proven capabilities:

### Wire Protocol Layer (`skadi-sql-gateway`)
- PostgreSQL wire protocol (pgwire) — full Tableau/psql/DBeaver compatibility
- MySQL wire protocol — COM_QUERY text protocol, `mysql_native_password` auth
- Extended query protocol: Parse/Bind/Describe/Execute/Sync
- SQL injection validation, secret redaction, require-SSL enforcement

### Authentication & Authorisation
- Trust / plaintext / bcrypt credential stores
- Per-user schema ACL (`PrincipalPolicy`) — row-level schema filtering
- Audit log: connect, query, error events (structured, never contains SQL)

### SQL Dialect & Execution
- `SqlDialectBridge`: PG → Databricks SQL, MySQL → Databricks SQL
- `SqlNormalizer`: deterministic whitespace/keyword/punctuation normalisation for cache keys
- `DatabricksJdbcExecutor`: connection pool, query timeout, cancel propagation
- `QueryGovernor`: per-user concurrency cap

### Caching
- In-memory TTL cache; Arrow IPC format (path 3) and text row format (path 1)
- Cache key: `SHA-256(user, normalized_sql, params)`
- Dataset version absent from cache keys (known gap L3)
- S3 cache tier exists in `skadi-server` but is not reachable from the gateway

### Observability
- Micrometer/Prometheus metrics: QPS, p50/p95/p99 latency, cache tier tags, active sessions, error SQLSTATE counters
- Structured MDC logging: `session_id`, `query_id`, `client`
- Correlation IDs forwarded to Databricks via `ApplicationName`

### Deployment
- Docker multi-stage image, `docker-compose.yml`, k8s manifest with readiness probe
- `PgWireHealthIndicator`, `MySqlWireHealthIndicator` at `/actuator/health/`
- Smoke-test script (`scripts/smoke-test.sh`)

### `skadi-server` (built, not yet connected)
- Standalone Spring Boot service with async Arrow HTTP API
- S3 cache tier: `CachedAwsSdkS3AccessLayer`, `ResultSetToS3ChunkWriter`
- Peer-cache HTTP replication
- OpenAPI contract: `docs/openapi-skadi-query-v1.yaml`
- **The gateway→server seam is the key unresolved structural gap.**

---

## 2. The Architectural Gap

The system-map intent (gateway → server → core) has not been fulfilled. The gateway calls
Databricks directly. `skadi-server` is built but unreachable. Every new feature added to the
gateway (observability, governance, caching) has to be re-implemented rather than shared.

The semantic layer evolution resolves this naturally: the semantic layer becomes the **first
real caller** of `skadi-server`'s HTTP API, finally activating the intended topology:

```
BI Tools / AI Chat / Dashboard Bricks
           ↓
  skadi-sql-gateway (SQL protocol path — unchanged)
           ↓
  skadi-semantic (new — semantic query → SQL compilation, governance)
           ↓  HTTP POST /api/v1/queries
  skadi-server (existing — Arrow execution, S3 cache — finally connected)
           ↓
  Databricks SQL Warehouse
```

The pgwire/MySQL endpoint continues to provide raw SQL access for Tableau and psql.
The semantic layer is an additional, governed access path — not a replacement.

---

## 3. Proposed Architecture Evolution

### Three new capabilities

#### 3.1 Semantic Query Layer (`skadi-semantic`)
A new service that sits between clients and `skadi-server`. It:
- Loads and validates semantic contract files (YAML — see ADR-005)
- Exposes a semantic query API: clients select metrics + dimensions + filters by name
- Compiles semantic queries to normalised Databricks SQL using the existing `SqlDialectBridge`
- Enforces governance at the semantic object level (dataset, metric, dimension access policies)
- Passes compiled SQL to `skadi-server` for execution and caching

The semantic layer has no execution engine of its own. It is a governance + compilation service.

#### 3.2 Dashboard Brick Platform (`skadi-ui-bricks` + `skadi-dashboard`)
A composable dashboard platform where:
- A **brick** is the atomic unit: semantic query + visualisation config + access policy + cache TTL
- A **dashboard** is an ordered composition of bricks with layout metadata
- Both bricks and dashboards are versioned YAML artifacts reviewed and published via a governance workflow
- A **brick runtime** (React component library) renders any brick from its config by fetching results from the semantic layer

The platform does not replace Tableau. It is the native governed dashboard surface for Skadi.

#### 3.3 AI Chat Integration
AI assistants query Skadi through the semantic query API — never through raw SQL.
The integration has three points:
1. **Intent resolution**: NL → structured semantic query (LLM translates user intent into metric + dimension selections from the known contract catalog)
2. **Execution**: semantic query runs through the semantic layer (same governance, same cache)
3. **Response generation**: LLM receives the Arrow result as a structured table and generates a natural language answer

AI governance is not a prompt-level concern — it is enforced structurally by the semantic layer API.

---

## 4. Reusable Components from Existing Work

| Existing Component | Reuse in Evolution |
|---|---|
| `SqlNormalizer` | Semantic layer uses it to normalise compiled SQL before passing to `skadi-server` |
| `SqlDialectBridge` | Semantic SQL compiler emits Databricks SQL via the bridge (MYSQL dialect or direct) |
| `DatabricksJdbcExecutor` | Used inside `skadi-server`; semantic layer delegates execution there |
| `QueryResultCache` / Arrow path | `skadi-server` cache is the semantic layer's cache — no new cache needed |
| `PrincipalPolicyRegistry` | Extended to semantic object access (datasets, metrics) — same principal model |
| `AuditLog` | Extended with `audit_semantic_query` and `source=ai_chat` fields |
| `GatewayMetrics` / Micrometer | Semantic layer adds its own meters; same Prometheus endpoint |
| `JdbcArrowStreamer` | Used inside `skadi-server`; no change |
| Deployment packaging | Same Dockerfile pattern; semantic service as additional compose service |

**Not reused / not changed:**
- `pgwire` and `mysqwire` packages — raw SQL path remains untouched
- `MetadataQueryRouter` — not semantic-layer-aware; stays as pgwire-internal
- Existing auth in `skadi-sql-gateway` — the semantic layer has its own auth model (API key or JWT, per ADR-007)

---

## 5. Phased Implementation Roadmap

### Lane C — Semantic Foundation

| Story | Description | Key Output |
|---|---|---|
| C1 | Semantic contract format, YAML schema, validation tooling | `skadi-semantic/contracts/` schema; CI validation |
| C2 | Semantic contract registry service startup and loading | `ContractRegistry` Spring bean; health indicator |
| C3 | Semantic query API endpoint (REST) | `POST /semantic/v1/query`; `SemanticQuery` request model |
| C4 | Semantic SQL compiler (metrics + dims → Databricks SQL) | `SemanticSqlCompiler`; unit tests with golden SQL |
| C5 | Gateway→server connection (activate `skadi-server` HTTP path) | `Skadi-server` called from semantic layer; end-to-end Arrow result |
| C6 | Semantic access policy enforcement | Principal → dataset/metric ACL check before compilation |

**Exit criteria:** a semantic query from a governed principal returns a correct Arrow result without the caller knowing any Databricks table name.

---

### Lane D — Dashboard Brick Platform

| Story | Description | Key Output |
|---|---|---|
| D1 | Brick definition format (YAML: semantic query + vis config) | `skadi-ui-bricks/schema/brick.schema.json`; examples |
| D2 | Brick registry service (load, validate, publish bricks) | `BrickRegistry`; REST API `GET /bricks/{id}` |
| D3 | Brick runtime — React component library | `skadi-ui-bricks` npm package; renders chart/table/metric-card |
| D4 | Dashboard definition format + runtime | `DashboardConfig` YAML; `DashboardRuntime` React app |
| D5 | Governance workflow (versioning, review, publish gate) | PR-based publish; brick state machine: draft → review → published |

**Exit criteria:** a dashboard composed of 3 bricks renders correctly, pulls from cache on second load, and rejects bricks the user's role cannot access.

---

### Lane E — AI Chat Integration

| Story | Description | Key Output |
|---|---|---|
| E1 | Contract catalog prompt context (metric/dim names for LLM) | `CatalogContextBuilder` — formats contract registry for LLM system prompt |
| E2 | Intent resolution endpoint (NL → semantic query) | `POST /ai/v1/intent`; Claude API integration; structured semantic query output |
| E3 | AI chat brick type | Dashboard brick that renders a chat input; streams semantic query result back |
| E4 | AI audit + governance integration | `source=ai_chat` audit events; semantic layer blocks out-of-policy intents |

**Exit criteria:** "What was the total PnL by book for yesterday?" resolves to a governed semantic query, executes, returns a natural language answer, and appears in the audit log.

---

## 6. Governance Principles

These principles apply across all three lanes:

**Semantic-layer-first for new query surfaces.** Raw SQL via pgwire is a legacy path for BI tools. All new query surfaces (dashboards, AI, APIs) go through the semantic layer.

**Governance at the API boundary, not the prompt.** AI governance is enforced by which semantic queries the AI is allowed to construct, not by what we tell the LLM to avoid. The semantic layer rejects unauthorised metric/dataset access structurally.

**Contracts are code.** Semantic contracts, brick definitions, and dashboard configs are YAML files in the repository. They are reviewed, version-controlled, and deployed like code — not edited through a UI.

**No vendor lock-in on the AI layer.** The intent resolution endpoint uses a pluggable LLM provider interface. Claude is the reference implementation; the interface must be provider-neutral.

**Cache reuse across surfaces.** A semantic query executed by Tableau (via pgwire → dialect bridge) and the same query executed by an AI chat brick should hit the same cache entry. Cache keys must be surface-agnostic.

---

## 7. Open Questions

| # | Question | Relevant ADR |
|---|---|---|
| Q1 | Does the semantic layer expose a pgwire interface (so Tableau can use semantic objects directly), or REST-only? | ADR-004 |
| Q2 | Is `skadi-semantic` a new Maven module or an additional route inside `skadi-server`? | ADR-004 |
| Q3 | What is the semantic contract schema version strategy when physical tables change? | ADR-005 |
| Q4 | Should bricks support server-side rendering for PDF export / email delivery? | ADR-006 |
| Q5 | Do AI chat responses cite their semantic query for auditability? | ADR-007 |
| Q6 | When `skadi-server` is finally connected, should the gateway route through it or remain direct-JDBC for the raw SQL path? | ADR-003, ADR-004 |

---

## Related ADRs

- [ADR-001](adr/ADR-001-sql-dialect.md) — SQL dialect decision (PG/MySQL wire compatibility)
- [ADR-002](adr/ADR-002-pgwire-netty.md) — pgwire implementation decision
- [ADR-003](adr/ADR-003-skadi-warehouse-lite-future-direction.md) — cache-aware SQL engine (future)
- [ADR-004](adr/ADR-004-semantic-query-layer.md) — semantic query layer
- [ADR-005](adr/ADR-005-semantic-contracts.md) — semantic contract format
- [ADR-006](adr/ADR-006-dashboard-brick-model.md) — dashboard brick model
- [ADR-007](adr/ADR-007-ai-chat-integration.md) — AI chat integration points
