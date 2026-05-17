# DQR-004: Full SQL Gateway / Semantic Execution Convergence

**Status:** Open
**Raised:** 2026-05-17 (split from DQR-002 after issue #97 resolved partial convergence)
**Blocking:** no current lane; future scope only
**Related:** DQR-002 (resolved), ADR-004, ADR-010, dev-status debt item L10
**Resolves in:** a dedicated future story after the semantic path has production soak

---

## Scope

DQR-002 resolved the question of how the semantic layer delegates to `skadi-server`
(Option 4 — partial convergence). One part of the original question was explicitly
**not decided** and is tracked here:

> Should the SQL gateway's raw SQL path also route through `skadi-server`, eliminating
> the dual-path topology and the two-pool / two-cache problem?

`skadi-sql-gateway` was **intentionally untouched** in Lane E. This DQR records why
that decision was made, what the remaining trade-offs are, and what would need to be
true before full convergence is attempted.

---

## Context

After Lane E E1 (issue #97), the execution topology is:

```
BI tool (Tableau/psql)
  └─► skadi-sql-gateway  ──► Databricks (direct JDBC, own pool, own cache)

Semantic API consumer
  └─► SkadiServerQueryExecutionService  ──► skadi-server  ──► Databricks
```

Two JDBC pools, two cache owners, two execution paths. This is the "partial
convergence" state DQR-002 Option 4 accepted as the pragmatic next step.

Debt item **L10** documents the longer-term concern:
> "Gateway embeds own JDBC pool + cache instead of delegating to `skadi-server` —
> duplicates execution concerns."

---

## The question

Should a future lane route SQL gateway queries through `skadi-server` as well,
converging to a single JDBC pool, a single cache, and a single execution authority?

## Options

| # | Option | Summary | Pros | Cons |
|---|---|---|---|---|
| 1 | Keep dual execution paths | Semantic execution delegates to `skadi-server`; SQL gateway remains direct | Lowest gateway risk; preserves known SQL client behavior | Two JDBC/cache owners persist; duplicated execution concerns remain |
| 2 | Converge SQL gateway through `skadi-server` | Gateway delegates execution to server query execution boundary | One JDBC/cache owner; cleaner platform architecture; resolves L10 more fully | Higher blast radius; requires careful Tableau/client regression testing |
| 3 | Introduce a shared execution library | Both server and gateway call a shared execution module in-process | Avoids network hop; reduces duplicated logic | Still complex; may blur module boundaries; deployment coupling increases |
| 4 | Phased convergence | Add optional gateway delegation behind config, soak, then migrate | Controlled migration; measurable risk reduction | More temporary complexity; requires dual-mode test coverage |

## Current leaning

Option 4 (phased convergence) is the safest likely path. The SQL gateway is a
compatibility surface for BI clients. Convergence should be tested behind a feature
flag or configuration switch, with SQL client regression coverage and
production-like soak before becoming the default.

---

## Why full convergence was deferred

1. **Blast radius.** `skadi-sql-gateway` is production-critical for Tableau Server /
   Cloud. Routing gateway traffic through an additional HTTP hop adds a new failure
   point for active users.
2. **Scope discipline.** Lane E was scoped to semantic execution activation only.
   Gateway changes are a separate architectural move with their own risk profile.
3. **Soak first.** The semantic-to-`skadi-server` delegation path added in Lane E
   should prove stable before the higher-traffic gateway path is rerouted.

---

## What full convergence would require

- A decision that `skadi-server` is the single JDBC/cache authority for all paths.
- A rollout plan that preserves Tableau connectivity throughout the migration.
- Either a feature flag or a blue/green approach to avoid a flag-day cutover.
- Extended test coverage for the gateway-through-server path under realistic Tableau
  query patterns (golden-result harness from B4 is relevant).
- Resolution of latency implications: the extra HTTP hop affects p50/p95 for
  interactive Tableau queries.

---

## Decision status

**Not yet decided.** Full convergence is explicitly future scope. This DQR should be
revisited after:

1. The Lane E semantic path has had meaningful production use.
2. The performance impact of the extra HTTP hop has been measured.
3. A future lane (candidate: Lane F or a dedicated convergence lane) is scoped.

Do not converge the SQL gateway in the absence of a deliberate decision here.
