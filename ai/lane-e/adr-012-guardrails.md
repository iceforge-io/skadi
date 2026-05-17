# ADR-012 Semantic Metadata Foundation — Fitness Tests and Guardrails

**Lane E story:** E1.10 (issue #81)
**Status:** Implemented — foundation guardrails active; future guardrails documented below
**Date:** 2026-05-17
**Related ADRs:** ADR-012, ADR-005, ADR-011

---

## 1. Purpose

ADR-012 establishes that the buddy chat must interrogate the governed semantic model
and must not maintain independent business definitions. Before any buddy-chat runtime
exists, Skadi enforces several foundation-level invariants through automated tests.

This document lists the active fitness tests and the future guardrails that should be
implemented as the platform evolves.

---

## 2. Active fitness tests

All tests are in `Adr012FitnessTest` (`skadi-semantic/src/test/java/...`).

### 2.1 Metadata preservation (load → registry)

| Test | Invariant |
|---|---|
| `adr012_enrichedContractExplanation_survivesJsonLoad` | Contract explanation fields survive JSON deserialization |
| `adr012_measureExplanation_survivesJsonLoad` | All measures in enriched contract carry explanation after load |
| `adr012_dimensionExplanation_survivesJsonLoad` | All dimensions carry explanation after load |
| `adr012_enrichedMetadata_survivesRegistryBoundary` | Explanation not stripped at `LoadedContractRegistry` boundary |
| `adr012_measureExplanation_survivesRegistryBoundary` | Measure explanation accessible via `registry.findByName()` |
| `adr012_dimensionExplanation_survivesRegistryBoundary` | Dimension explanation accessible via `registry.findByName()` |

### 2.2 Null-safety (missing optional metadata)

| Test | Invariant |
|---|---|
| `adr012_missingExplanation_isNull_notException` | Minimal contract has `null` explanation — no NPE |
| `adr012_missingMeasureExplanation_isNull_notException` | Measure explanation is null in minimal contract — no NPE |
| `adr012_missingDimensionExplanation_isNull_notException` | Dimension explanation is null in minimal contract — no NPE |

### 2.3 Specific metadata accessibility

| Test | Invariant |
|---|---|
| `adr012_lineageHook_accessible_withHookId` | Lineage hook ID is accessible for compliance tracking |
| `adr012_entitlementBehavior_accessible_withMode` | Entitlement behavior mode is present as a placeholder seam |
| `adr012_validGroupingPaths_accessible` | Valid grouping paths are non-empty in enriched contract |
| `adr012_outputShapes_accessible` | Output shapes are non-empty in enriched contract |

### 2.4 Governance metadata correctness

| Test | Invariant |
|---|---|
| `adr012_nonGroupableDimension_declaredInContract` | Non-groupable (`desk`) dimension correctly declared |
| `adr012_groupableDimension_declaredInContract` | Groupable (`legal_entity`) dimension correctly declared |

### 2.5 Enriched fixture completeness

| Test | Invariant |
|---|---|
| `adr012_enrichedFixture_coversAllRequiredExplanationFields` | All contract-level fields present in `enriched-contract.json` |
| `adr012_enrichedFixture_measureExplanation_coversRequiredFields` | intendedUsage + caveats present on all measures |
| `adr012_enrichedFixture_dimensionExplanation_coversRequiredFields` | description present on all dimensions |

### 2.6 Backward compatibility

| Test | Invariant |
|---|---|
| `adr012_minimalContract_remainsValid` | `sample-contract.json` loads and validates without explanation |
| `adr012_minimalContractMeasures_haveNullExplanation` | Minimal contract measures carry `null` explanation |
| `adr012_inMemoryMinimalContract_noExplanationRequired` | `SemanticContract` constructs with `null` explanation |

---

## 3. Future guardrails (TODOs)

The following guardrails are not yet implemented. They should be added as the platform matures.

### 3.1 ArchUnit rule — no independent business definitions

**When to add:** When Lane E2 introduces buddy-chat or any component that could author
business definitions independently.

**What to enforce:** An ArchUnit rule preventing any class outside `org.iceforge.skadi.semantic.contract`
from defining business measure/dimension labels, descriptions, grain, or caveats as string constants
or hardcoded values. This blocks AI meaning sprawl at the source code level.

```
// Future ArchUnit check (pseudocode):
noClasses()
    .that().resideOutsideOfPackage("..semantic.contract..")
    .should().haveSimpleNameEndingWith("Glossary")
    .orShould().accessFieldWhere(field -> field.isAnnotatedWith(BusinessDefinition.class))
```

### 3.2 Contract linting warning for missing explanation metadata

**When to add:** When ADR-012 compliance becomes a governance requirement (e.g. before
promoting Lane E to production).

**What to add:** A new `ContractValidationSeverity.INFO` (or `WARNING`) issue code:
- `CONTRACT_MISSING_OWNER` — contract has no `explanation.owner`
- `CONTRACT_MISSING_GRAIN` — contract has no `explanation.grain`
- `CONTRACT_MISSING_INTENDED_USAGE` — contract has no `explanation.intendedUsage`
- `MEASURE_MISSING_INTENDED_USAGE` — measure has no `explanation.intendedUsage`

These should be `WARNING` severity so they surface in `GET /api/semantic/contracts/validation`
without blocking contract registration.

### 3.3 Fitness test — validation endpoint does not execute queries

**When to add:** When Lane E2 introduces a semantic query execution path.

**What to enforce:** A Spring Boot integration test verifying that
`POST /api/semantic/v1/query/validate` completes without invoking any
`QueryExecutionService`, `DatabricksJdbcExecutor`, or cache write operation.

### 3.4 Fitness test — metadata API returns governed values only

**When to add:** When a buddy-chat runtime or LLM integration is introduced.

**What to enforce:** A contract test verifying that the metadata API responses
(`GET /api/semantic/v1/contracts/{contractId}`) contain only values that come from
the active `ContractRegistry` — not from hardcoded strings, AI-generated content,
or external knowledge bases.

---

## 4. Summary

| Layer | Status |
|---|---|
| Contract model — explanation metadata | ✅ Implemented (E1.1) |
| Enriched fixture coverage | ✅ Implemented (E1.2) |
| Registry preservation | ✅ Implemented (E1.3) |
| Metadata API endpoints | ✅ Implemented (E1.6) |
| Request validation endpoint | ✅ Implemented (E1.8) |
| Fitness tests for all of the above | ✅ Implemented (E1.10) |
| ArchUnit AI meaning sprawl guard | ⬜ Future (see §3.1) |
| Missing-metadata linting warnings | ⬜ Future (see §3.2) |
| Validation endpoint no-execution test | ⬜ Future (see §3.3) |
| Governed-values-only metadata test | ⬜ Future (see §3.4) |
