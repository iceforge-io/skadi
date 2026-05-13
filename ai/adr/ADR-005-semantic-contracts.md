# ADR-005: Semantic Contracts

**Status:** Proposed
**Date:** 2026-05-13
**Owners:** engineering, data governance
**Related ADRs:** ADR-004, ADR-006
**Supersedes:** —

---

## 1. Context

### Problem

For the semantic query layer (ADR-004) to compile and govern queries, it needs a formal
description of:
- Which Databricks tables/views are exposed as logical datasets
- What metrics are defined on each dataset (named SQL aggregations)
- What dimensions are available (grouping and filtering columns)
- Who is allowed to query each dataset, metric, and dimension
- What cache behaviour applies (TTL, invalidation hints)

Without a machine-readable contract format, these definitions live implicitly in dashboard
SQL, are inconsistent across teams, and cannot be enforced programmatically.

### Background

- Skadi already has `SqlGatewayProperties` as the pattern for config-as-code — contracts follow the same principle
- The existing `PrincipalPolicyRegistry` (per-user schema ACL) is the access model predecessor
- Dataset versioning is a known gap (L3 in `ai/current-system-state.md`); the contract format must accommodate it
- dbt, LookML, and Cube.dev have each solved similar problems; the format should be informed by them but not depend on them

---

## 2. Decision

Semantic contracts are **YAML files**, versioned in the repository under
`skadi-semantic/contracts/`. They are the authoritative source of semantic model definitions.

### Contract file structure

Each contract file defines one **dataset** — a logical view over one or more physical Databricks tables.

```yaml
# skadi-semantic/contracts/mxl_risk.yaml

dataset: mxl_risk               # logical name, used in semantic queries
version: "1.0"                  # contract schema version (semver)
description: "MXL risk gold layer — daily PnL and Greeks"

binding:
  catalog: main
  schema:  risk
  table:   gold_risk             # or a SQL view expression (see 'expression' variant)

metrics:
  - name:        pnl
    label:       "Total PnL"
    expression:  "SUM(pnl)"
    type:        DECIMAL
    description: "Sum of daily profit and loss"

  - name:        delta_risk
    label:       "Delta Risk"
    expression:  "SUM(delta)"
    type:        DECIMAL

dimensions:
  - name:    cob_date
    column:  cob_date
    type:    DATE
    label:   "Close of Business Date"
    filterable: true
    groupable:  true

  - name:    book
    column:  book
    type:    STRING
    label:   "Trading Book"
    filterable: true
    groupable:  true

  - name:    desk
    column:  desk
    type:    STRING
    label:   "Trading Desk"
    filterable: true
    groupable:  true

cache:
  ttl:           "2h"
  strategy:      ttl             # ttl | dataset-version (dataset-version requires versioning resolver)
  invalidate_on: []              # future: ["dataset_version_change"]

access:
  default_policy: deny           # deny | allow
  roles:
    - role:    risk_analyst
      grants:  [query]           # query | admin
    - role:    risk_viewer
      grants:  [query]
    - role:    risk_admin
      grants:  [query, admin]
```

### Contract loading and validation

- `skadi-semantic` loads all `*.yaml` files from the contracts directory at startup
- A JSON Schema (`contracts/schema/dataset.schema.json`) validates each file before it is registered
- If any contract fails validation, the application fails to start (no silent ignoring)
- The `ContractRegistry` bean exposes contracts for the compiler and governance enforcer

### Contract versioning

- `version` is a semver string on the contract itself (not the dataset data version)
- Contract schema versions are backward-compatible within a major version
- A breaking change to a contract (renaming a metric, removing a dimension) requires a major version bump and a migration note
- Contracts are code — changes go through PR review the same as any other code change

### Dataset version binding (future, story C3)

The `strategy: dataset-version` option enables cache keys to include the physical dataset version
(Delta snapshot ID or a Skadi-managed version token). This resolves the known gap L3.
For Phase 1 (Lane C), `strategy: ttl` is the only implemented option.

---

## 3. Rationale

**Why YAML rather than a DSL or proprietary format?**
YAML is readable, diffable in PRs, and widely understood. A custom DSL adds a compiler and
tooling burden. LookML is an example of a DSL that requires significant tooling investment —
avoid that cost until the value of a DSL is proven.

**Why one file per dataset?**
Single-dataset files stay small, produce focused diffs, and make access policy review
straightforward. A monorepo of contracts is easier to navigate than a single large file.

**Why not use dbt metrics layer or Cube.dev YAML directly?**
Those formats are designed for their own ecosystems. Adopting them would couple Skadi to
external tools and their deployment models. The Skadi contract format is informed by those
designs but is owned by the Skadi codebase.

**Why deny-by-default access policy?**
Skadi operates in regulated environments (financial risk data). An allow-by-default posture
would require active opt-out for every new dataset — too easy to forget. Deny-by-default
means new datasets are inaccessible until access is explicitly granted.

---

## 4. Consequences

### Positive
- Metric definitions are single-source-of-truth, shared across all query surfaces (dashboards, AI, direct API)
- Access policy review happens at contract PR time, before deployment
- Dataset version awareness (story C3) can be added to the contract format without changing the API
- Contracts provide the prompt context for AI intent resolution (ADR-007) — the LLM knows metric names and descriptions

### Negative / Risks
- Every new dataset or metric requires a contract file — this is intentional friction but requires a clear contribution process
- Contract expressions are raw SQL fragments — they can contain dialect-specific constructs that break if the physical schema changes
- Validation at startup means a bad contract blocks the entire service; a per-contract fallback mode may be needed for large contract sets

### Operational Impact
- Contracts directory must be mounted into the `skadi-semantic` container (ConfigMap in k8s, volume mount in compose)
- Contract changes require a service restart to take effect (no hot-reload in Phase 1)
- CI pipeline should validate contracts on every PR (`./mvnw validate` or a standalone schema check)

---

## 5. Alternatives Considered

| Option | Why Rejected |
|--------|--------------|
| Database-stored contracts (UI-editable) | Contracts become config drift risk; no PR review; harder to version and roll back |
| dbt metrics layer YAML | Requires dbt deployment and its own execution model; adds external dependency |
| Cube.dev schema | Cube is a full semantic layer product; adds a large external runtime dependency and vendor surface |
| Code-defined contracts (Java annotations) | Less readable for non-Java contributors; harder for governance review; no standalone validation |

---

## 6. Fitness Functions / Enforcement

- CI job validates all `contracts/*.yaml` files against `dataset.schema.json` on every PR
- Startup hard-fail if any contract file fails validation
- A test that adds an invalid contract file and asserts the application fails to start
- A test that loads a valid contract and asserts the `ContractRegistry` contains the expected metrics and dimensions

> No automated runtime drift detection in Phase 1. Review-only for contract content correctness.

---

## 7. Migration / Rollout Plan

1. Define `dataset.schema.json` (the JSON Schema for contract YAML files) — this is the first deliverable
2. Write contracts for the first dataset(s) — risk team contribution
3. `ContractRegistry` loads and exposes contracts; `skadi-semantic` validates at startup
4. Dataset version binding (contract `strategy: dataset-version`) deferred to story C3

---

## 8. Open Questions

- Q1: Should contracts support SQL `expression`-level bindings (subqueries/views) in addition to simple `catalog.schema.table` bindings?
- Q2: What is the escalation path when a metric expression must change in a backward-incompatible way (rename, type change)?
- Q3: Should dimension `filterable` and `groupable` flags be enforced at API level (the compiler rejects grouping on a non-groupable dimension), or advisory only?
- Q4: Who owns contracts — the data engineering team or the consuming team? A contribution process needs to be defined.
