# ADR-009: Semantic Contracts Before Semantic Planning

**Status:** Accepted
**Date:** 2026-05-14
**Owners:** engineering, architecture
**Related ADRs:** ADR-004, ADR-005, ADR-008, ADR-010
**Related Issues:** #38 (Lane C epic), #44 (C6 ADRs/DQRs)
**Supersedes:** —

---

## 1. Context

### Problem

ADR-004 describes a semantic query layer that compiles metric and dimension selections into
Databricks SQL. ADR-003 describes a future query optimiser (Skadi Warehouse Lite). There is
a temptation to implement the semantic compiler and planner together — both deal with query
transformation — or to build the planner first because it seems architecturally simpler than
defining a full contract vocabulary.

Without a stable contract vocabulary (dataset names, metric names, dimension names, access
policy, cache hints), any planner implementation must embed assumptions about query structure
that are not yet agreed. If those assumptions are wrong, the planner must be rewritten when
contracts are finally defined. The same risk applies to the UI Brick Runtime (Lane D) and AI
Chat Buddy (Lane E): both need a stable `SemanticQuery` type and `ContractRegistry` interface
before they can be built.

### Background

- The current platform has no semantic vocabulary. Every client (Tableau, psql, DBeaver)
  speaks raw SQL against physical Databricks tables.
- `skadi-server` has an Arrow execution path but no concept of a metric, dimension, or
  dataset.
- ADR-005 (Proposed) defines a YAML-based contract format. The format is still an open
  question (DQR-001), but the *need* for a contract type is not.
- ADR-006 (Dashboard Brick Model) requires `query.dataset`, `query.metrics`, and
  `query.dimensions` to reference semantic contract names. Without contracts, brick
  definitions are unvalidatable.
- ADR-007 (AI Chat Integration) requires `ContractRegistry.forPrincipal()` to build the
  LLM context window. Without a registry interface, intent resolution cannot be tested
  against the contract vocabulary.

---

## 2. Decision

**Semantic contracts and output-shape metadata are defined before any semantic planner or
query optimizer is introduced.**

Concretely:

1. **C2 introduces `SemanticContract` and `ContractRegistry`** — the vocabulary and
   registry interface. No loading mechanism and no planner.
2. **C3 introduces `SemanticQuery` and `SemanticCompiler` interface** — the query record
   and compilation interface. The compiler interface accepts an optional `PlanHints`
   parameter to leave the extension seam open, but `PlanHints` in Lane C is always an
   empty/no-op record.
3. **The semantic planner / rule engine is introduced only after contracts are stable** —
   meaning after the contract format (DQR-001) is resolved and at least one real contract
   file exists.
4. **`SemanticCompiler` implementations in Lane C are stubs** — they return fixture SQL
   and do not invoke `SqlDialectBridge`.

### What C2/C3 are allowed to introduce

- Plain Java records: `SemanticContract`, `Metric`, `Dimension`, `AccessPolicy`,
  `SemanticQuery`, `OutputShape`, `PlanHints` (empty record)
- Interfaces: `ContractRegistry`, `SemanticCompiler`
- No Spring beans, no file loading, no compilation logic

### What remains out of scope until post-Lane C

- Semantic planner / rule engine
- Multi-step query plan generation
- Query optimization hints derived from physical statistics
- Routing decisions (which Databricks cluster, which cache tier)
- Semantic compiler wired to `SqlDialectBridge`

---

## 3. Rationale

**Why contracts before planner?**
A planner needs something to plan over. Without a `SemanticContract` that defines legal
metrics, dimensions, and their types, a planner would hardcode assumptions about schema
structure. Those assumptions would couple the planner to a specific physical schema, making
it brittle to changes. The correct dependency order is:

```text
contracts (vocabulary) → compiler (transforms vocab to SQL) → planner (optimises SQL plans)
```

Not:

```text
planner → assumes schema → fragile to contract changes
```

**Why stabilise contracts before connecting to `skadi-server`?**
The cache key in `skadi-server` is derived from normalised SQL. If the semantic compiler
changes how it generates SQL (because contract fields were renamed during the planning
phase), cache entries are silently invalidated or, worse, stale entries match new queries.
Stable contracts → stable compiled SQL → reliable cache behaviour.

**Why is `PlanHints` an empty record in Lane C?**
The `SemanticCompiler` interface signature is `compile(SemanticQuery, PlanHints)`. Including
`PlanHints` in Lane C costs nothing and preserves the extension seam for the planner.
Omitting it and adding it later would require changing the interface signature after Lane D
and E code already calls it — a breaking change.

**Why does this support Lane D and Lane E?**
Lane D (UI Brick Runtime) defines bricks as `query.dataset + query.metrics + query.dimensions`.
These field names reference `SemanticContract` names. If `SemanticContract` is undefined or
unstable, brick YAML validation in CI cannot work.

Lane E (AI Chat Buddy) builds a LLM context window from
`ContractRegistry.forPrincipal(principal)`. If the registry interface is undefined, the
`CatalogContextBuilder` cannot be written or tested.

Both lanes block on C2/C3.

---

## 4. Consequences

### Positive

- Lane D and Lane E have a stable vocabulary before implementation starts
- Cache correctness is preserved: compiled SQL is stable because contracts are stable
- The planner extension seam (`PlanHints`) is reserved without the planner being built
- Contract format (DQR-001) can be resolved after C2/C3 without changing the Java types
- Governance review of contract *definitions* is decoupled from governance review of
  planner *logic* — two separate PR lifecycles

### Negative / Risks

- The semantic compiler stub in C3 returns fixture SQL — callers that test against it
  must replace the stub in a post-Lane C story before real compilation works
- `PlanHints` as an empty record will look odd to reviewers; a comment explaining its
  purpose as an extension seam is required
- If a team attempts to build the planner before contract format is resolved (DQR-001),
  they will find the `ContractRegistry` interface has no load path — this is intentional
  friction, not a bug

### Operational Impact

- No runtime impact in Lane C — all additions are offline types and stubs
- Post-Lane C: when the semantic compiler is connected to `SqlDialectBridge`, the
  compiled SQL output must be regression-tested against the golden-result harness
  introduced in B4

---

## 5. Alternatives Considered

| Option | Why Rejected |
| --- | --- |
| Build semantic compiler and contracts in a single story | Conflates vocabulary definition with compilation logic; makes it harder to review contracts independently; couples two concerns that should be separately stable |
| Build planner first, derive contracts from planner output | Inverts the dependency graph; planner would encode physical schema assumptions; contracts would become planner artefacts rather than governance artefacts |
| Skip Lane C and go directly to full ADR-004 implementation | ADR-004 requires `ContractRegistry`, `SemanticQuery`, `OutputShape` — these are Lane C C2/C3 deliverables. Skipping Lane C means building ADR-004 on types that were never reviewed independently. |
| Use dbt metrics layer as the contract format and build planner on top | Adds dbt as a required external dependency; the dbt execution model is incompatible with Skadi's delegated execution architecture; governance would live outside the codebase |

---

## 6. Fitness Functions / Enforcement

- `SemanticCompiler` implementations in Lane C must return a hardcoded fixture SQL string,
  not invoke `SqlDialectBridge`. A code review check is sufficient for Lane C.
- `PlanHints` must remain an empty (or effectively no-op) record until the planner story
  is started. Any non-empty field in `PlanHints` before that point is a scope violation.
- The `ContractRegistry` interface must not have a `loadFromDirectory(Path)` or
  `loadFromFile(Path)` method in Lane C — loading is deferred to post-DQR-001 resolution.

> No automated ArchUnit enforcement planned for Lane C. Review-only.

---

## 7. Migration / Rollout Plan

1. **C2 (Lane C):** `SemanticContract`, `Metric`, `Dimension`, `AccessPolicy`,
   `ContractRegistry` interface — vocabulary only
2. **C3 (Lane C):** `SemanticQuery`, `OutputShape`, `PlanHints`, `SemanticCompiler`
   interface with stub implementation
3. **Post-DQR-001 (post Lane C):** implement `ContractRegistry` loading from the decided
   format; write first real contract files
4. **Post-Lane C:** connect `SemanticCompiler` stub to real `SqlDialectBridge`; add
   compiler correctness tests against golden SQL output
5. **Post-Lane C:** introduce semantic planner as a `PlanHints` producer; wire into
   `SemanticCompiler.compile(SemanticQuery, PlanHints)`

---

## 8. Open Questions

- Q1: Should `PlanHints` be defined in `skadi-semantic` or in `skadi-core` (shared
  module)? If the planner and the compiler live in different modules, `PlanHints` needs
  to be on a shared classpath.
- Q2: Should `SemanticCompiler.compile()` be a pure function (no I/O) or is async
  compilation needed for future multi-step plans? Lane C stubs are pure; the interface
  may need to change for async planners.
