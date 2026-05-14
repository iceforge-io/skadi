# ADR-008: Lane C — Contracts, Skeletons, and Boundaries

**Status:** Accepted
**Date:** 2026-05-14
**Owners:** engineering, architecture
**Related ADRs:** ADR-004, ADR-005, ADR-006, ADR-007, ADR-009, ADR-010
**Related Issues:** #38 (Lane C epic), #39 (C1 platform boundary model), #44 (C6 ADRs/DQRs)
**Supersedes:** —

---

## 1. Context

### Problem

Lane A and Lane B produced a working, production-hardened SQL Gateway
(`skadi-sql-gateway`) and a built-but-uncalled execution engine (`skadi-server`).
Four ADRs (004–007) describe a future semantic layer, dashboard brick model, and AI chat
integration. Without an explicit scope boundary, Lane C risks drifting into premature
implementation of the semantic planner, UI runtime, LLM integration, or entitlement engine —
building systems that depend on contract formats and interface shapes that have not yet been
stabilised.

### Background

- ADR-004 through ADR-007 are all **Proposed**, not Accepted. Their implementation
  depends on contracts and interfaces that do not yet exist.
- `skadi-server` is built but has never been called by `skadi-sql-gateway`. Activating
  that seam without defined contracts would couple the two modules prematurely.
- The platform has no `SemanticContract`, `ContractRegistry`, `SemanticQuery`,
  `SemanticCompiler`, or `SemanticExecutor` type. Lanes D and E cannot be started until
  these exist and are stable.
- Lane C is the prerequisite for Lanes D and E. It is not a delivery lane for
  user-visible features.

### Drivers

- Prevent scope creep that would couple implementation details before boundaries are clear
- Give Lane D (UI Brick Runtime) and Lane E (AI Chat) a stable interface surface to build
  against
- Maintain a clear audit trail of what was decided (ADRs) vs. what remains open (DQRs)

---

## 2. Decision

**Lane C is intentionally limited to architectural contracts, skeletal Java interfaces
and records, documentation, test fixtures, and offline tests.**

### What is built in Lane C

| Story | Deliverable |
| --- | --- |
| C1 | Documentation: platform boundary model, component owns/does-not-own tables, diagram |
| C2 | `SemanticContract` record; `ContractRegistry` interface |
| C3 | `SemanticQuery` record; `OutputShape` record; `SemanticCompiler` interface |
| C4 | `CacheContract` record; `CacheIdentity` record |
| C5 | `SemanticExecutor` interface; `StubSemanticExecutor`; `SkadiserverSemanticExecutor` skeleton |
| C6 | ADRs 008–010 and DQRs 001–003 |
| C7 | Offline serialization and structural tests |
| C8 | Dev-status update; Lane C runbook |

### What is NOT built in Lane C

- No semantic query planner or rule engine
- No YAML/JSON contract file loader (DQR-001 tracks the contract storage format as an open
  question — not decided here)
- No live Databricks connection from the candidate `skadi-semantic` module
- No UI runtime, React components, or dashboard rendering
- No LLM integration, intent resolution, or AI response generation
- No production entitlement engine
- No behavioral changes to production runtime code in `skadi-sql-gateway` or `skadi-server`
  (later Lane C stories may add isolated Java records, interfaces, stubs, fixtures, and
  tests in the candidate semantic module only)
- No convergence of the gateway's direct JDBC path with `skadi-server` (tracked as
  DQR-002)

### Module scope

The candidate module `skadi-semantic` (name confirmed in C2) contains only:

- Plain Java records and interfaces — no Spring beans in C2–C4
- `StubSemanticExecutor` and the `SkadiserverSemanticExecutor` skeleton in C5,
  optionally wired as Spring beans behind a `skadi.semantic.executor=stub` flag
- No production-traffic-serving code

`skadi-sql-gateway` and `skadi-server` receive no behavioral changes — no production
runtime code in either module is modified at any point in Lane C.

---

## 3. Rationale

**Why contracts before implementation?**
Contracts are the shared vocabulary between the semantic compiler, the cache layer, the UI
brick runtime, and the AI chat intent resolver. Building any of those systems before the
vocabulary is stable creates coupling that is expensive to unwind. The correct sequence is:
define the interface → build implementers against it → connect.

**Why defer contract file loading and format?**
The storage format (YAML, JSON, pure Java, database) affects the contribution workflow,
CI validation, and contract lifecycle. Getting it wrong is not free to undo once multiple
teams have written contracts. DQR-001 captures this as an open question; the format decision
is not made in Lane C.

**Why not extend `skadi-sql-gateway` with semantic stubs?**
The gateway is a protocol adapter with per-session TCP state. Semantic contract types are
stateless data structures. Mixing them creates a dependency from a production-critical
process into experimental contract code. The `skadi-semantic` module is isolated so that
it can fail at startup without affecting gateway uptime.

**Why placeholder skeletons for `SkadiserverSemanticExecutor`?**
Having a named skeleton in the codebase makes the integration point explicit and
discoverable. A bare `// TODO` comment in an unrelated file is invisible to future agents
and reviewers. A named class with a `TODO: activate HTTP call` comment is a search target.

---

## 4. Consequences

### Positive

- Lane D and Lane E have stable Java interfaces to build against after Lane C completes
- Scope creep is structurally prevented: there is no production service to extend in Lane C
- ADRs 008–010 and DQRs 001–003 capture the decisions and open questions that would
  otherwise accumulate as implicit knowledge
- The platform boundary model (C1) and these ADRs together form an onboarding document
  for future contributors and AI coding agents

### Negative / Risks

- No user-visible features are delivered in Lane C — stakeholders expecting runnable
  behaviour will be disappointed
- The `SkadiserverSemanticExecutor` skeleton may attract premature implementation attempts;
  the guardrail in ADR-008 and the `TODO` comment must be respected
- If the contract format decision (DQR-001) is delayed beyond Lane C, C2 records may
  need minor adjustments when YAML/JSON loading is added

### Operational Impact

- No build changes in `skadi-sql-gateway` or `skadi-server` modules
- The candidate `skadi-semantic` module (if introduced as a Maven module in C2) adds a
  new submodule to the parent POM — no runtime service is started
- No deployment changes, no new environment variables, no infrastructure changes

---

## 5. Alternatives Considered

| Option | Why Rejected |
| --- | --- |
| Build the full semantic compiler in Lane C | Requires contract format to be decided first; builds implementation on unstable interfaces; contradicts the contracts-first principle |
| Extend `skadi-sql-gateway` with semantic awareness | Wrong module boundary; gateway is a protocol adapter, not a semantic layer; creates coupling to experimental code in a production-critical process |
| Defer Lane C entirely and go straight to Lane D | Lane D requires `ContractRegistry`, `SemanticQuery`, and `POST /semantic/v1/query` to exist; without Lane C, Lane D has no stable surface to build against |
| Build all ADR-004–007 systems in Lane C | ADR-004–007 are Proposed, not Accepted; their detailed decisions depend on stable contracts; building before stabilising is premature |

---

## 6. Fitness Functions / Enforcement

- The `skadi-sql-gateway` and `skadi-server` Maven modules must have zero changed source
  files after any Lane C commit. Reviewers check this via `git diff --name-only` on each
  PR.
- The candidate `skadi-semantic` module must not contain a `main` class or a runnable
  Spring Boot application entry point in Lane C.
- No `@Service`, `@Component`, or `@Repository` annotations in C2–C4 classes. C5 may
  add a single `@Bean` factory behind a config flag.

> No automated ArchUnit enforcement planned for Lane C. Review-only.

---

## 7. Migration / Rollout Plan

Lane C is purely additive. The recommended execution order is:

1. C1: documentation — platform boundary model (no code)
2. C6: ADRs and DQRs — capture boundary decisions before any code is written
3. C2: `SemanticContract` record and `ContractRegistry` interface
4. C3: `SemanticQuery`, `OutputShape`, `SemanticCompiler` interface
5. C4: `CacheContract` and `CacheIdentity` records
6. C5: `SemanticExecutor` interface, `StubSemanticExecutor`, `SkadiserverSemanticExecutor` skeleton
7. C7: offline serialization and structural tests
8. C8: dev-status update and Lane C runbook

Lane D begins after C8 is merged and the C8 runbook has been reviewed.

---

## 8. Open Questions

- Q1: Should the candidate `skadi-semantic` module be a new Maven submodule of
  `skadi-parent` from C2, or introduced as a standalone later? (Pragmatic question;
  does not affect interface definitions.)
- Q2: When should ADR-004 status be updated from Proposed to Accepted — after Lane C
  interface stabilisation or after Lane D first calls the endpoint?
