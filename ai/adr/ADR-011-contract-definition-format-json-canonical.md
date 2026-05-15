# ADR-011: Contract Definition Format — JSON as Canonical Runtime Format

**Status:** Accepted
**Date:** 2026-05-14
**Owners:** engineering, data governance
**Related ADRs:** ADR-005 (Proposed — YAML was leading candidate), ADR-008, ADR-009, ADR-010
**Related Issues:** #63 (Lane D: D2), #61 (Lane D epic)
**Related DQRs:** DQR-001 (Resolved — see decision section there)
**Supersedes:** DQR-001 open question (resolves it for Lane D)

---

## 1. Context

### Problem

Lane C (`skadi-semantic`) delivered Java records for the semantic contract model and JSON test
fixtures that exercise Jackson serialization round-trips. Lane D must load contract definitions
from files to populate `ContractRegistry`. Before any file-backed loader (D3) can be written,
the storage format must be decided.

DQR-001 tracked this as an open question. ADR-005 (Proposed) named YAML as the leading candidate
but was never accepted. As Lane D begins, the format question must be resolved so D3 can proceed.

### Background

- Lane C JSON fixtures (`sample-contract.json`, `sample-query-contract.json`,
  `sample-cache-boundary.json`) already define the canonical JSON shape for `SemanticContract`,
  `SemanticQueryContract`, and cache boundary types.
- Jackson 2.19.2 (Spring Boot 3.5.6 BOM) handles Java record deserialization natively — no extra
  annotations or compiler flags required.
- `skadi-semantic` uses Jackson in test scope; D3 will require it in compile scope.
- ADR-005 was written before the Lane C records existed. Its YAML example maps to a richer
  authoring format, not the serialized record shape. Those are separable concerns.
- DQR-001 identified the following risk if format is decided wrong: pure Java records exclude
  non-engineer contributors; database-stored contracts lose PR review lifecycle; YAML without
  schema validation allows silent errors.

### Drivers

- D3 (file-backed loader) is the immediate blocker — it cannot be implemented until the format
  is decided.
- Lane C tests already validate JSON round-trips for every contract type; the format is de facto
  proven for the Java record shape.
- AI coding agents edit JSON reliably and deterministically; YAML indentation errors and
  ambiguous types (e.g., bare `ON`/`OFF` parsed as boolean) are common sources of fixture rot.
- Strict JSON syntax (no implicit type coercions, no multi-document streams, no anchors/aliases)
  simplifies loader error reporting.

---

## 2. Decision

**JSON is the canonical format for Lane D semantic contract files.**

Specific decisions:

| Question | Decision |
|---|---|
| Runtime contract file format | JSON |
| In-memory model | Java records (`SemanticContract` etc. — unchanged from Lane C) |
| YAML support | Deferred — possible future human-authoring layer; not required for D3 |
| JSON Schema validation | Deferred to D6 (structural validation) or later |
| File naming convention | `<contract-name>.json` under a configurable contracts directory |
| Canonical example | `skadi-semantic/src/test/resources/fixtures/sample-contract.json` (Lane C fixture) |

### What is decided

- D3 implements a `ContractLoader` that reads `*.json` files, deserializes them via Jackson into
  `SemanticContract` instances, and returns them to the caller.
- The existing Lane C JSON fixtures are the canonical contract file examples — no new example
  file is required.
- Jackson is promoted from test scope to compile scope in `skadi-semantic` pom.xml (D3 story).
- D3 must not add a YAML parser dependency.

### What is NOT decided or deferred

- YAML as a human-authoring layer is **deferred, not rejected.** A future story may add a YAML
  → JSON preprocessing step or a YAML loader variant. That story would be responsible for adding
  the YAML parser dependency and updating this ADR to Amended status.
- JSON Schema validation is **deferred to D6** (contract validation and diagnostics story).
  D3 relies on Jackson deserialization errors to surface malformed files. D6 will add
  structural validation rules beyond what Jackson enforces.
- One-file-per-contract vs. multi-contract files: D3 assumes one `SemanticContract` per JSON
  file. A multi-contract envelope format is a future extension.

---

## 3. Rationale

### Why JSON over YAML for Lane D

| Factor | JSON | YAML |
|---|---|---|
| Lane C test coverage | Fixtures already exist and pass | No YAML fixtures exist |
| Jackson support | Native record deserialization via compile-scope jackson-databind | Requires jackson-dataformat-yaml or SnakeYAML; adds a parser decision |
| Syntax strictness | Strict — quotes, braces, commas are unambiguous | Relaxed — `ON`, `OFF`, `yes`, `no` parsed as boolean; indent-sensitive |
| AI/agent round-trips | Deterministic — agents produce consistent JSON | Indentation errors are common; YAML anchors/aliases can confuse agents |
| Error messages | Jackson errors are precise (line/col, field name) | SnakeYAML errors less consistently formatted |
| Source-control diffs | Compact and unambiguous | Human-readable but larger diffs for nested structures |
| ADR-005 precedent | Proposed YAML; never Accepted | — |

### Why not pure Java records

Java record definitions as contract authoring format (Option 3 in DQR-001) require Java
developer involvement for every contract change. Data governance contributors — risk managers,
analysts, data engineers — cannot edit a Java file without a build and deployment cycle.
Contracts must be files readable and editable without IDE or compiler setup.

### Why not database-stored

Database-stored contracts (Option 4 in DQR-001) lose the PR review lifecycle that is the
primary governance control for access policy changes. Contracts must be version-controlled.

### Why JSON rather than deferring further

The DQR-001 risk summary stated: if format is delayed past post-Lane C, C2 `ContractRegistry`
stubs cannot be replaced with real implementations, and Lane D bricks cannot validate brick
files against a real contract registry. The format must be decided in D2 so D3 can proceed.
JSON is the most conservative choice: it requires no new parser dependency beyond what Jackson
already provides, and the shape is already validated by Lane C tests.

---

## 4. Consequences

### Positive

- D3 can be implemented immediately using Jackson, which is already in the dependency graph.
- Existing Lane C JSON fixtures serve as canonical contract file examples — no migration required.
- JSON contract files are readable by any JSON tool, IDE plugin, or CI validator without
  additional configuration.
- Strict parsing means malformed contracts fail loudly at load time (D3 throws on
  `JsonProcessingException`), which is the correct behavior.
- Future YAML authoring layer can be added transparently: a pre-processing step that converts
  YAML → JSON before the D3 loader reads it.

### Negative / Risks

- JSON is more verbose than YAML for deeply nested structures. Contract files may be harder to
  read for non-engineers compared to a YAML equivalent. This is acceptable for Lane D; the YAML
  authoring layer addresses it in a later story.
- Data engineers accustomed to dbt or Helm YAML will need to write JSON. A comment syntax is
  not supported in JSON (no `//` or `#` comments). The ADR-005 YAML example used comments
  extensively — the JSON format cannot replicate that inline.
- If YAML is chosen as the authoring format in a future lane, the D3 loader must be extended or
  replaced. The `ContractLoader` interface (D3) must be designed so the JSON implementation is
  swappable.

### Operational Impact

- `skadi-semantic/pom.xml` gains `jackson-databind` in compile scope (changed from test scope
  in D3).
- Contract files must be placed in a configurable directory (`skadi.semantic.contracts.path` or
  equivalent) accessible at runtime — classpath, filesystem mount, or both.
- No new external system dependency beyond Jackson. No YAML parser, no JSON Schema library.
- CI validation of contract files: D3 tests load fixtures from `src/test/resources`; CI catches
  invalid JSON before any runtime deployment.

---

## 5. Alternatives Considered

| Option | Disposition |
|---|---|
| YAML files (ADR-005 leading candidate) | Deferred — not rejected. Adds parser dependency; no Lane C fixtures. Future authoring layer candidate. |
| JSON first, YAML later (hybrid) | This is exactly the decision: JSON now (D3), YAML later (post-Lane D). Not an alternative; it is the chosen approach. |
| Pure Java records as authoring format | Rejected — excludes non-Java contributors; requires rebuild/redeploy for any contract change. |
| Database-stored contracts | Rejected — no PR review lifecycle; drift risk; operational infrastructure dependency. |
| dbt metrics YAML | Rejected — requires dbt deployment; format designed for dbt execution model. |
| JSON with inline JSON Schema (`$schema` field) | Deferred to D6 — adds validation complexity without a loader. Acceptable future extension. |

---

## 6. Fitness Functions / Enforcement

- D3 tests load `sample-contract.json` and assert the resulting `SemanticContract` matches
  expected field values. This double-registers the fixture as both a loader test and a format
  conformance check.
- Any PR that adds a `jackson-dataformat-yaml` or `snakeyaml` dependency to `skadi-semantic`
  (compile scope) before a YAML-decision ADR is accepted must be rejected in review.
- `git diff --check` catches trailing whitespace in contract JSON files.

> No automated ArchUnit enforcement planned for Lane D. Review-only.

---

## 7. Migration / Rollout Plan

1. **D2 (this ADR)** — format decision recorded; DQR-001 marked resolved.
2. **D3** — `ContractLoader` interface + JSON file-backed implementation; Jackson promoted to
   compile scope; tests load from classpath fixtures.
3. **D6** — `ContractValidator` adds structural validation beyond Jackson parsing errors
   (missing required fields, duplicate names, invalid cross-references).
4. **Post-Lane D (optional)** — YAML authoring layer: a YAML → Java record conversion step,
   backed by a new ADR amending this one, with a separate YAML parser dependency decision.

---

## 8. Open Questions

- Q1: Should contract files live under `skadi-semantic/src/main/resources/contracts/` (classpath)
  or be configurable as a filesystem path? D3 should support both; this does not affect the
  format decision.
- Q2: Should multi-contract JSON envelopes (an array of contracts in one file) be supported in
  Lane D? Recommendation: no — one file per contract; simpler loader error attribution.
- Q3: When YAML authoring is added, should the YAML → JSON conversion happen at load time
  (transparent to the registry) or at CI time (committed JSON only)? This is a future lane
  decision.
