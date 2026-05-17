# Lane E1 — Semantic Contract Explanation Metadata

**Lane E story:** E1.1–E1.4 (issues #72–#75, parent epic #82)
**Status:** Implemented
**Module:** `skadi-semantic` (`org.iceforge.skadi.semantic.contract`)
**Date:** 2026-05-17
**Related ADRs:** ADR-012, ADR-011, ADR-005

---

## 1. Purpose

ADR-012 extends Skadi semantic contracts from execution-only definitions into
**explainable, metadata-bearing contracts**. The buddy chat and other downstream
consumers must be able to answer questions about data meaning, grain, filters,
lineage, and entitlements by interrogating the governed semantic model — not by
inventing answers from LLM general knowledge or a separate knowledge base.

This document describes the explanation metadata fields added in Lane E1 and how
they are represented in JSON contract files and Java records.

---

## 2. Why explanation metadata exists

Without explanation metadata, a semantic contract defines:
- what measures exist (name, expression, type)
- what dimensions exist (name, column, type, groupable, filterable)
- what access policy applies
- what cache policy applies

That is sufficient for **query execution** but not for **semantic explanation**. When a
user asks "What does 1-Day VaR mean?", "What grain is this data?", "Why can't I drill
into desk?", or "Who owns this definition?" — the contract must carry that information.

Lane E1 adds optional explanation metadata at three levels:
- **Contract level** — ownership, grain, valid grouping paths, output shapes,
  lineage hooks, entitlement behavior, intended usage, and caveats
- **Measure level** — intended usage and caveats for each measure
- **Dimension level** — description and caveats for each dimension

All fields are optional. Existing contracts that do not include explanation metadata
remain valid and continue to function for query execution.

---

## 3. Java record model

All types live in `org.iceforge.skadi.semantic.contract`.

### 3.1 `SemanticContractExplanation`

Contract-level explanation metadata. All fields are nullable; Jackson serializes
only non-null fields (via `@JsonInclude(NON_NULL)`).

| Field | Type | Purpose |
|---|---|---|
| `owner` | `String` | Team or individual responsible for this contract definition |
| `intendedUsage` | `String` | Guidance on when and how to use this contract |
| `caveats` | `List<String>` | Known limitations, ambiguities, or operational gotchas |
| `grain` | `String` | Row-level granularity of the underlying dataset |
| `outputShapes` | `List<String>` | Names of typical result shapes this contract produces |
| `lineageHook` | `SemanticLineageHook` | Placeholder identifying the upstream lineage hook |
| `entitlementBehavior` | `SemanticEntitlementBehavior` | Placeholder describing access entitlement behavior |
| `validGroupingPaths` | `List<String>` | Dimension names or comma-joined paths forming valid GROUP BY combinations |

### 3.2 `SemanticMeasureExplanation`

Measure-level explanation metadata, carried as an optional `explanation` field on
`SemanticMeasure`. All fields are nullable.

| Field | Type | Purpose |
|---|---|---|
| `intendedUsage` | `String` | Guidance on when and how to use this measure |
| `caveats` | `List<String>` | Measure-specific limitations or calculation assumptions |

### 3.3 `SemanticDimensionExplanation`

Dimension-level explanation metadata, carried as an optional `explanation` field on
`SemanticDimension`. All fields are nullable.

| Field | Type | Purpose |
|---|---|---|
| `description` | `String` | Business description of what the dimension represents |
| `caveats` | `List<String>` | Dimension-specific limitations or behavioral edge cases |

### 3.4 `SemanticLineageHook` (placeholder)

Identifies an upstream lineage hook. Both fields are nullable. Enforcement is out
of scope for Lane E1.

| Field | Type | Purpose |
|---|---|---|
| `hookId` | `String` | Stable identifier for the lineage hook (e.g. `RISK-LIN-VAR-01`) |
| `description` | `String` | Human-readable description of the lineage requirement |

### 3.5 `SemanticEntitlementBehavior` (placeholder)

Describes how access entitlements are applied to this contract. Both fields are
nullable. Enforcement is out of scope for Lane E1.

| Field | Type | Purpose |
|---|---|---|
| `mode` | `String` | Entitlement mode (e.g. `ROW_FILTER`, `COLUMN_MASK`, `FULL`) |
| `description` | `String` | Human-readable description of the entitlement behavior |

---

## 4. JSON shape

Explanation metadata is expressed as an optional `explanation` object at the
contract, measure, and dimension levels. Fields that are not specified are omitted
from the JSON file; they deserialize to `null` and do not affect backward
compatibility.

### 4.1 Annotated JSON example

```json
{
  "name": "cib_var",
  "version": { "value": "2.0.0" },
  "description": "CIB VaR gold layer — daily 1-day and 10-day Value at Risk",
  "entity": {
    "name": "cib_var",
    "description": "CIB VaR gold layer physical table",
    "endpoint": { "catalog": "main", "schema": "risk", "table": "gold_var" },
    "ruleRefs": [
      { "ruleId": "BCBS239-01", "description": "Data lineage requirement" }
    ]
  },
  "measures": [
    {
      "name": "var_1d",
      "label": "1-Day VaR (99%)",
      "expression": "SUM(var_1d_99)",
      "type": "DECIMAL",
      "description": "1-day Value at Risk at 99% confidence level",
      "explanation": {
        "intendedUsage": "Use for daily regulatory capital consumption reporting.",
        "caveats": [
          "Assumes normal distribution — tail events may exceed stated VaR",
          "Reported in USD; local currency positions converted at end-of-day FX rates"
        ]
      }
    }
  ],
  "dimensions": [
    {
      "name": "legal_entity",
      "column": "legal_entity_code",
      "type": "STRING",
      "label": "Legal Entity",
      "filterable": true,
      "groupable": true,
      "explanation": {
        "description": "Short code identifying the regulated legal entity (e.g. CIBUK, CIBNA).",
        "caveats": [
          "Legal entity codes may change during restructuring; historical codes are preserved"
        ]
      }
    },
    {
      "name": "desk",
      "column": "desk_id",
      "type": "STRING",
      "label": "Trading Desk",
      "filterable": true,
      "groupable": false,
      "explanation": {
        "description": "Internal desk identifier. Grouping by desk is not permitted for regulatory reporting."
      }
    }
  ],
  "accessPolicy": {
    "allowedRoles": [{ "name": "risk_analyst" }],
    "allowedPrincipals": [{ "type": "GROUP", "name": "cib-risk-team" }],
    "ruleRefs": [{ "ruleId": "GOV-FRTB-01", "description": "FRTB governance policy" }]
  },
  "cachePolicy": { "strategy": "TTL", "ttlSeconds": 14400, "ruleRefs": [] },
  "explanation": {
    "owner": "CIB Risk Technology",
    "intendedUsage": "Use for daily regulatory VaR reporting under Basel III and FRTB.",
    "caveats": [
      "Data is available after 8 AM UTC; early-morning queries may return the previous COB",
      "FX rates used for normalisation are end-of-day rates from the market data golden source"
    ],
    "grain": "One row per legal entity, risk class, desk, and close-of-business date",
    "outputShapes": ["var_by_legal_entity", "var_by_risk_class"],
    "lineageHook": {
      "hookId": "RISK-LIN-VAR-01",
      "description": "BCBS 239 compliant lineage hook for CIB VaR aggregation pipeline"
    },
    "entitlementBehavior": {
      "mode": "ROW_FILTER",
      "description": "Rows filtered by the caller's legal entity entitlement set"
    },
    "validGroupingPaths": [
      "cob_date",
      "legal_entity",
      "risk_class",
      "cob_date,legal_entity",
      "cob_date,risk_class",
      "legal_entity,risk_class",
      "cob_date,legal_entity,risk_class"
    ]
  }
}
```

### 4.2 Minimal contract (no explanation metadata)

Explanation metadata is fully optional. The following minimal contract remains valid
and functionally unchanged after Lane E1:

```json
{
  "name": "mxl_risk",
  "version": { "value": "1.0.0" },
  "description": "MXL risk gold layer — daily PnL and Greeks",
  "entity": {
    "name": "mxl_risk",
    "description": "MXL risk gold layer entity",
    "endpoint": { "catalog": "main", "schema": "risk", "table": "gold_risk" },
    "ruleRefs": []
  },
  "measures": [
    {
      "name": "pnl",
      "label": "Total PnL",
      "expression": "SUM(pnl)",
      "type": "DECIMAL",
      "description": "Sum of daily profit and loss"
    }
  ],
  "dimensions": [
    {
      "name": "book",
      "column": "book",
      "type": "STRING",
      "label": "Trading Book",
      "filterable": true,
      "groupable": true
    }
  ],
  "accessPolicy": { "allowedRoles": [], "allowedPrincipals": [], "ruleRefs": [] },
  "cachePolicy": { "strategy": "NONE", "ttlSeconds": null, "ruleRefs": [] }
}
```

---

## 5. Backward compatibility

Explanation metadata is **fully optional**:

- Existing contracts without `explanation` fields deserialize with `explanation == null`
  at the contract, measure, and dimension levels.
- `null` explanation fields are omitted from serialized JSON output
  (`@JsonInclude(NON_NULL)` on each record class).
- The `ContractLoader`, `ContractRegistryPopulator`, `ContractRegistry`, and all existing
  tests continue to work without modification.
- No existing fixture or test was changed to accommodate Lane E1 explanation metadata.

---

## 6. Filtering and grouping behavior

The `filterable` and `groupable` flags on `SemanticDimension` predate Lane E1 and
remain the authoritative governance signal for query validation:

| Flag | Meaning |
|---|---|
| `filterable: true` | The semantic query layer may accept this dimension as a filter predicate |
| `filterable: false` | Filtering by this dimension is not permitted in governed queries |
| `groupable: true` | The semantic query layer may include this dimension in GROUP BY |
| `groupable: false` | Grouping by this dimension is not permitted in governed queries |

Lane E1 adds `validGroupingPaths` at the contract explanation level to document
**multi-dimension grouping combinations** that are semantically valid (e.g. you
may group by `cob_date,legal_entity` but not by `desk` alone for regulatory
reporting). This is representational metadata — enforcement is deferred to a later
lane.

---

## 7. Lineage hooks and entitlement behavior (placeholders)

`SemanticLineageHook` and `SemanticEntitlementBehavior` are **representation-only
placeholders** in Lane E1. They carry identifiers and descriptions that future
platform components can act on, but Lane E1 does not add enforcement logic.

| Placeholder | Future use |
|---|---|
| `lineageHook` | Will link to the upstream data pipeline lineage system to satisfy BCBS 239 and similar regulatory data lineage requirements |
| `entitlementBehavior` | Will inform row-filter, column-mask, or deny decisions in the access policy enforcement layer (planned for Lane B or a dedicated governance lane) |

---

## 8. Fixtures

| Fixture | Location | Metadata |
|---|---|---|
| `sample-contract.json` | `skadi-semantic/src/test/resources/fixtures/` | Minimal — no explanation fields |
| `enriched-contract.json` | `skadi-semantic/src/test/resources/fixtures/` | Fully enriched — all explanation fields populated |

The enriched fixture (`cib_var`, version 2.0.0) is the canonical reference example
for Lane E1 explanation metadata. It covers a CIB VaR dataset with two measures
(`var_1d`, `var_10d`) and four dimensions (`cob_date`, `legal_entity`, `risk_class`,
`desk`).

---

## 9. Registry access

Explanation metadata is preserved through the full contract lifecycle:

```
JSON file → JsonContractLoader → SemanticContract → ContractRegistryPopulator
         → LoadedContractRegistry → findByName() / forPrincipal()
```

`LoadedContractRegistry` stores `SemanticContract` records directly without
transformation. No metadata is stripped at the registry boundary. See
`ContractRegistryPopulatorTest` for the registry-level assertions.

---

*Cross-reference: ADR-012 §4 (Explanation metadata model), ADR-011 (JSON canonical
format), ADR-005 (Semantic contracts).*
