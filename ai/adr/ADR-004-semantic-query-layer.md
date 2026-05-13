# ADR-004: Semantic Query Layer

**Status:** Proposed
**Date:** 2026-05-13
**Owners:** engineering, architecture
**Related ADRs:** ADR-001, ADR-002, ADR-005, ADR-007
**Supersedes:** —

---

## 1. Context

### Problem

The current platform gives clients raw SQL access to Databricks tables via the pgwire/MySQL wire
protocol. This works well for BI tools (Tableau, psql, DBeaver) but is not suitable as the
foundation for governed dashboards or AI query access because:

- Clients must know physical table names, column names, join conditions, and Databricks SQL dialect
- Governance operates at schema ACL level only — there is no concept of a governed metric or dimension
- AI assistants generating raw SQL bypass semantic intent, are fragile to schema changes, and are hard to audit
- There is no shared metric definition; each dashboard reimplements `SUM(pnl)` independently with no consistency guarantee
- The gateway→server seam (the original architectural intent) has never been activated — `skadi-server` is built but uncalled

### Background

- `skadi-sql-gateway` calls Databricks directly; `skadi-server` (with its Arrow HTTP API and S3 cache) is never invoked
- `skadi-server` has a complete OpenAPI contract (`docs/openapi-skadi-query-v1.yaml`) defining `POST /api/v1/queries`
- `SqlDialectBridge` already compiles PG/MySQL SQL to Databricks SQL — it can be reused to compile semantic query output
- `SqlNormalizer` already produces deterministic SQL for cache keys — same normalisation applies to compiled semantic SQL
- The semantic layer is not the same as ADR-003 (Skadi Warehouse Lite): it does not execute SQL natively; it delegates to Databricks

---

## 2. Decision

We will introduce a **semantic query layer** as a new Spring Boot module (`skadi-semantic`) that:

1. **Loads and validates semantic contracts** (see ADR-005) at startup from a configured contracts directory
2. **Exposes a semantic query REST API** (`POST /semantic/v1/query`) that accepts metric names, dimension names, filter expressions, and principal identity — not raw SQL
3. **Compiles semantic queries to Databricks SQL** using the `SqlDialectBridge` and normalises output via `SqlNormalizer`
4. **Enforces governance** before compilation: rejects requests where the principal lacks access to the requested dataset, metric, or dimension
5. **Delegates execution to `skadi-server`** via `POST /api/v1/queries`, activating the original gateway→server topology for the first time
6. **Returns Arrow IPC results** streamed from `skadi-server`; callers receive the same Arrow format as the existing HTTP path

The semantic layer does **not**:
- Execute SQL itself — it delegates entirely to `skadi-server`
- Replace the pgwire/MySQL raw SQL path — that remains unchanged for Tableau and psql
- Build a query optimiser or planner — that is ADR-003 scope
- Define visualisation or rendering — that is ADR-006 scope
- Implement AI intent resolution — that is ADR-007 scope
- Own or duplicate the cache — `skadi-server` is the cache owner

### Resulting topology

```
BI Tools (Tableau, psql)
        ↓
skadi-sql-gateway  ─────────────────────────────────→ Databricks (direct JDBC, unchanged)

Dashboard Bricks / AI Chat
        ↓
skadi-semantic  (new)
        ↓  POST /api/v1/queries
skadi-server  ─────────────────────────────────────→ Databricks (JDBC + Arrow + S3 cache)
```

The two paths are independent and share no runtime state. The raw SQL path is not deprecated.

---

## 3. Rationale

**Why a separate module rather than extending `skadi-sql-gateway`?**
The gateway is a protocol adapter optimised for wire-protocol session handling. The semantic
layer is a compilation and governance service with no per-session TCP state. These concerns
do not belong together.

**Why delegate to `skadi-server` rather than building a new execution path?**
`skadi-server` already has a working Arrow execution engine, S3 cache, and peer replication.
Bypassing it would mean either duplicating that work or leaving it permanently unused. The
semantic layer finally activates the original architectural intent.

**Why not extend `skadi-server` directly with semantic capabilities?**
`skadi-server` is an execution and caching engine. Embedding compilation, contract management,
and governance in it would violate its single responsibility. The seam between compilation
(semantic layer) and execution (`skadi-server`) is the correct place to separate concerns.

**Why REST rather than pgwire for the semantic API?**
REST with Arrow streaming is simpler to consume from dashboard bricks and AI clients. The
pgwire path remains available for clients that need it. If there is demand for a pgwire
semantic surface (e.g., Tableau accessing semantic objects directly), that can be added as a
translation layer in `skadi-sql-gateway` in a later story (see Open Question Q1).

---

## 4. Consequences

### Positive
- Governance at the semantic object level — principals query metrics, not tables
- First real activation of `skadi-server`, validating the gateway→server seam
- Shared metric definitions eliminate inconsistent dashboard logic
- AI chat, dashboards, and direct semantic API clients share the same cache entries
- Physical schema changes only require updating the contract file, not every downstream query

### Negative / Risks
- New module (`skadi-semantic`) adds deployment complexity
- Two query paths (raw SQL and semantic) must be documented and operated separately
- If `skadi-server`'s HTTP API has latency or reliability issues, those propagate to semantic queries (no direct-JDBC fallback on this path)
- Semantic compilation errors (bad contract, missing dimension) produce a new failure mode absent from the raw SQL path

### Operational Impact
- New deployable unit: `skadi-semantic` Spring Boot service
- `skadi-server` must be deployed and reachable from `skadi-semantic` (network, auth)
- The `skadi-server` HTTP API must be enabled (it can currently start without being called)
- Health indicators needed: semantic layer liveness + `skadi-server` reachability probe

---

## 5. Alternatives Considered

| Option | Why Rejected |
|--------|--------------|
| Extend `skadi-sql-gateway` with a semantic mode | Gateway is wire-protocol-session code; semantic compilation is stateless; wrong module boundary |
| Embed semantic layer in `skadi-server` | Violates single responsibility; execution engine should not own governance contracts |
| Add a LookML/dbt-style external semantic tool | Adds an external dependency and vendor surface; Skadi's governance model would live outside the codebase |
| Keep raw SQL but add predefined view aliases | Does not solve the governance or AI access problem; still exposes physical schema to clients |

---

## 6. Fitness Functions / Enforcement

- Contract loading fails fast at startup if any contract YAML is invalid (no silent ignoring)
- A governance rejection test: a principal without dataset access receives HTTP 403 with a `semantic_access_denied` error code (never exposing the physical table name)
- Integration test: semantic query → `skadi-server` → cached Arrow result → correct row count; asserts no raw SQL is passed to clients
- The semantic layer must never log or return physical table names in error messages to unauthorised principals

> No automated ArchUnit enforcement planned for Phase 1. Review-only.

---

## 7. Migration / Rollout Plan

1. **C1–C4 (Lane C):** build semantic layer with in-process stub for `skadi-server` (returns fixture Arrow data); validate compilation + governance independently
2. **C5 (Lane C):** activate the real `skadi-server` HTTP call; end-to-end test
3. **C6 (Lane C):** enforce access policies; smoke-test with two principals (one allowed, one denied)
4. Raw SQL path via pgwire/MySQL is never removed; both paths coexist

---

## 8. Open Questions

- Q1: Should the semantic layer eventually expose a pgwire interface so Tableau can query by metric name without knowing SQL?
- Q2: Is `skadi-semantic` a new Maven module alongside `skadi-server`, or a route group inside `skadi-server`?
- Q3: When `skadi-server` is finally connected, should the gateway's raw SQL path also route through `skadi-server` (deduplicating the JDBC pool), or stay direct?
