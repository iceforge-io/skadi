# DQR-001: Contract Definition Format

**Status:** Open
**Raised:** 2026-05-14
**Blocking:** post-Lane C loading implementation (C2+ code that loads contracts from storage)
**Related:** ADR-005 (Proposed), ADR-008, ADR-009
**Resolves in:** C6 analysis or early post-Lane C story

---

## Scope

What format should be used to define semantic contracts (datasets, metrics, dimensions,
access policy, cache hints)?

The Java type `SemanticContract` (introduced in C2) is format-agnostic. This question is
about the *storage and authoring* format: how a data engineer writes a contract, how it is
version-controlled, and how `skadi-semantic` loads it at startup.

This question does **not** affect C2–C5 Lane C deliverables. Those stories define Java
records and interfaces; loading is explicitly deferred until this question is answered.

---

## Impact

The answer affects:

- **Contribution workflow** — how a data engineering team writes and reviews a new contract
- **CI validation** — how the pipeline checks contract correctness before merge
- **Startup behaviour** — how `skadi-semantic` validates and loads contracts at boot
- **LLM context building** — whether the contract file format can be read directly as
  prompt context (ADR-007 intent resolver)
- **Contract versioning** — semver on the contract *schema* vs. semver on each file
- **Tooling** — whether standard YAML/JSON tooling (schema validators, IDEs) can be used
  out of the box

---

## Tie-in

- **ADR-005 (Proposed)** proposes YAML with JSON Schema validation. That ADR is Proposed,
  not Accepted — the format is the leading candidate but not final.
- **ADR-009** requires that `ContractRegistry` in C2 has no `loadFromDirectory(Path)`
  method. The loading implementation waits for this question.
- **C7 tests** use JSON fixtures for serialization round-trips (format-neutral test).
  If YAML is chosen, C7 tests can be extended to cover YAML loading without changing the
  Java records.
- **ADR-007** (AI Chat, Lane E) notes that the contract context window is formatted for
  the LLM from `ContractRegistry.forPrincipal()`. The registry interface abstracts the
  storage format from the LLM path — this DQR does not affect Lane E code.

---

## Options

| # | Option | Summary | Pros | Cons |
| --- | --- | --- | --- | --- |
| 1 | **YAML files** (ADR-005 proposal) | One `.yaml` file per dataset under `contracts/`; JSON Schema validates each file at startup | Human-readable, PR-diffable, matches `SqlGatewayProperties` config-as-code pattern; IDE support via JSON Schema | Requires startup YAML parser dependency; hot-reload not supported in Phase 1; YAML indentation errors are subtle |
| 2 | **JSON files** | One `.json` file per dataset | Machine-readable, strict syntax, wide tooling support | Less readable for non-engineers writing contracts; verbosity discourages adoption |
| 3 | **Pure Java records** | Contracts are Java classes instantiated in a `@Configuration` class | No separate format, no parser, contracts go through code review naturally | Non-engineers cannot contribute contracts; contract changes require a Java developer; loses separation between governance and code |
| 4 | **Database-stored** | Contracts stored in a relational or document store; editable via API | Dynamic updates without restart | No PR review lifecycle; drift risk; adds operational infrastructure dependency; contradicts config-as-code pattern |
| 5 | **dbt metrics YAML** | Reuse dbt's metrics layer format | Standard in the analytics ecosystem; tooling exists | Requires dbt deployment; format designed for dbt execution model; couples Skadi to an external tool |

---

## Current Leaning

**Option 1 (YAML files)** is the current leading candidate per ADR-005. The config-as-code
pattern is already established in this project (`SqlGatewayProperties`, `application.yml`).
YAML's human readability is important for governance review by non-engineers.

Concerns to resolve before confirming:

- Which YAML parser library is acceptable in `skadi-semantic` (Jackson YAML module vs.
  SnakeYAML vs. other)?
- Should JSON Schema validation use a Java JSON Schema library or an external CI tool?
- How are contract file paths configured (classpath vs. filesystem mount vs. both)?

---

## Risk Summary

**If decided wrong:**

- Pure Java records (Option 3) would exclude non-engineer governance contributors and
  make contract definitions invisible to tooling that reads YAML/JSON.
- Database-stored contracts (Option 4) would lose the PR review lifecycle that is the
  primary governance control in this platform.
- YAML (Option 1) with no validation schema would allow silent contract errors to reach
  production; the JSON Schema validation step in ADR-005 is the mitigation.

**If delayed past post-Lane C:**

- C2 `ContractRegistry` stubs cannot be replaced with real implementations.
- Lane D (brick YAML referencing `query.dataset`) cannot validate brick files against
  a real contract registry in CI.
- Lane E intent resolver cannot be tested against a real contract corpus.

---

## Decision Status

**Not yet decided.** YAML (Option 1) is the leading candidate per ADR-005. Confirmed
format and loading mechanism to be recorded in a follow-on ADR or as an ADR-005 status
change from Proposed to Accepted.
