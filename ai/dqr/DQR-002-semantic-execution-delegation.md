# DQR-002: Semantic Execution Delegation

**Status:** Resolved — Option 4 (partial convergence)
**Raised:** 2026-05-14
**Resolved:** 2026-05-17 — issue #97, PR #102, commit `42e4a4a`
**Related:** ADR-004, ADR-008, ADR-010, dev-status debt item L10
**Follow-on:** DQR-004 tracks the remaining full SQL gateway convergence question

---

## Scope

When and how should the future `skadi-semantic` layer delegate query execution to
`skadi-server`?

This question has two parts:

1. **Topology** — should `skadi-semantic` call `skadi-server` via REST
   (`POST /api/v1/queries`), or should it call Databricks directly via JDBC, or should
   some other arrangement be used?
2. **Convergence** — should the raw SQL path (SQL Gateway → Databricks direct) eventually
   also route through `skadi-server`, eliminating the dual-path topology?

Lane C C5 introduces `SkadiserverSemanticExecutor` as a skeleton class with a
`TODO: activate HTTP call` comment. The actual HTTP call and any topology changes are
deferred until this question is answered.

---

## Impact

The answer affects:

- **JDBC pool management** — whether there is one pool (in `skadi-server`) or two
  (gateway + semantic layer each owning a pool)
- **Cache sharing** — whether raw SQL and semantic queries share a single cache tier or
  maintain independent caches
- **Deployment topology** — whether `skadi-server` must always be reachable from
  `skadi-semantic`; latency and reliability implications
- **Debt L10** — the known issue that the gateway embeds its own JDBC pool and cache
  instead of delegating to `skadi-server`
- **`skadi-server` activation** — `skadi-server` is built but has never received real
  traffic; this question determines when that changes

---

## Tie-in

- **ADR-004 (Proposed)** decides this question: `skadi-semantic` calls `skadi-server` via
  `POST /api/v1/queries`. ADR-004 is Proposed, not Accepted — the decision is recorded
  but not yet implemented.
- **ADR-004 Open Question Q3** asks: "When `skadi-server` is finally connected, should
  the gateway's raw SQL path also route through `skadi-server`?" This DQR tracks that
  question explicitly.
- **ADR-010** (cache positioning) decides that both paths share cache via normalised SQL
  at the SQL level. This is achievable only if both paths hit the same cache owner
  (`skadi-server`). If the gateway continues to call Databricks directly and maintain its
  own cache, the two-cache problem persists.
- **Dev-status debt L10**: "Gateway embeds own JDBC pool + cache instead of delegating to
  `skadi-server` — duplicates execution concerns; resolved by Lane C (semantic layer
  activates `skadi-server`)." Lane C's resolution is the skeleton (`SkadiserverSemanticExecutor`);
  activation is post-Lane C.

---

## Options

| # | Option | Summary | Pros | Cons |
| --- | --- | --- | --- | --- |
| 1 | **`skadi-semantic` → `skadi-server` via REST** (ADR-004 proposal) | Semantic layer calls `POST /api/v1/queries`; `skadi-server` owns JDBC pool and cache | Activates `skadi-server`; single cache owner; ADR-004 already describes this topology | Adds a network hop; `skadi-server` must be reliably reachable from `skadi-semantic`; latency increases |
| 2 | **`skadi-semantic` calls Databricks directly via JDBC** | Semantic layer maintains its own JDBC pool | Simpler deployment (no `skadi-server` dependency); lower latency | Duplicates JDBC pool; two cache owners; cache sharing breaks; contradicts ADR-004 and ADR-010 |
| 3 | **Converge gateway through `skadi-server`** (full convergence) | Both gateway and semantic layer delegate JDBC to `skadi-server` | Single JDBC pool and cache; eliminates dual-path topology; resolves L10 fully | Significant gateway change; gateway is production-critical; convergence scope is large; risky |
| 4 | **Partial convergence — semantic layer via `skadi-server`, gateway stays direct** | Implement Option 1 for semantic path only; leave gateway direct path unchanged | Lower risk than full convergence; activates `skadi-server`; resolves ADR-004; defers gateway change | Two JDBC pools and two cache owners persist; L10 partially resolved; technical debt continues |

---

## Resolution

**Option 4 chosen** — partial convergence, implemented in issue #97.

- `SkadiServerQueryExecutionService` now makes real HTTP calls to `POST /api/v1/queries`
  on `skadi-server`. Cache-hit (200 SUCCEEDED) and async (202 ACCEPTED + status poll)
  paths are both handled.
- `skadi-sql-gateway` was **intentionally left unchanged** — the gateway's direct JDBC
  path continues unaffected. This was an explicit guardrail for Lane E.
- Full SQL gateway convergence (Option 3) is a separate future question. It should
  only be attempted after the semantic path has proven stable in production. See DQR-004.

---

## Risk Summary (for reference)

**`skadi-server` unavailable (the live risk under Option 4):**

- Semantic queries fail; raw SQL gateway path continues unaffected.
- A circuit breaker or health check is a future hardening item — not in scope for Lane E.

**Full convergence (Option 3) — still not done:**

- Gateway change is high-blast-radius; production Tableau connections affected.
- Deferred to DQR-004 and a future lane after production soak.

---

## Decision Status

**Resolved.** Option 4 implemented in issue #97 (commit `42e4a4a`, PR #102).
Full gateway convergence tracked separately in DQR-004.
