# DQR-002: Semantic Execution Delegation

**Status:** Open
**Raised:** 2026-05-14
**Blocking:** post-C5 activation of `SkadiserverSemanticExecutor`
**Related:** ADR-004, ADR-008, ADR-010, dev-status debt item L10
**Resolves in:** first post-Lane C story that activates live semantic execution

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

## Current Leaning

**Option 4** (partial convergence) is the most pragmatic next step:

- Implement `SkadiserverSemanticExecutor` to call `POST /api/v1/queries` as ADR-004
  proposes.
- Leave the gateway's direct JDBC path unchanged for now — the gateway is
  production-critical and convergence is a separate risk to manage.
- Plan full convergence (Option 3) as a later dedicated story after the semantic path
  is proven stable.

This matches the ADR-004 rollout plan (C5 activates `skadi-server` for semantic path)
while not over-committing to gateway changes.

---

## Risk Summary

**If delegated to `skadi-server` (Options 1 or 4) and `skadi-server` is unavailable:**

- Semantic queries fail; raw SQL path (gateway) continues unaffected.
- A circuit breaker or health check in `skadi-semantic` is required to prevent
  cascading failures.

**If semantic layer calls Databricks directly (Option 2):**

- Cache sharing between paths breaks; two independent caches produce stale-result risks
  and inflated Databricks costs.
- Contradicts ADR-010's cache positioning decision.

**If full convergence is attempted prematurely (Option 3):**

- The gateway change is high-blast-radius; production Tableau connections are affected.
- Full convergence should follow a production soak period of the semantic path.

**If delayed indefinitely:**

- `skadi-server` remains built but uncalled — a permanent dead weight in the codebase.
- Cache deduplication between the two paths is never achieved.
- Lane D bricks cannot use the live semantic endpoint.

---

## Decision Status

**Not yet decided.** Option 4 is the current leaning. The decision is confirmed when
the post-Lane C story to activate `SkadiserverSemanticExecutor` is started. At that
point, this DQR is marked Resolved and the activation approach is recorded in the
commit message or a new ADR.
