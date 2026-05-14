# C2 — Semantic Contract Skeletons: Implementation Notes

**Lane C story:** C2 (issues #55–#60, parent #40, epic #47)  
**Status:** Complete  
**Module:** `skadi-semantic`  
**Date:** 2026-05-14

---

## 1. Purpose

C2 establishes the semantic contract vocabulary for the Skadi platform. It is the
prerequisite for all later Lane C stories and for Lanes D and E.

The goal is narrow and deliberate: define the **data structures** that describe a
governed dataset (its metrics, dimensions, access policy, and cache policy) before
implementing any execution path. By stabilising the vocabulary first, C3 through C5
can build against a stable API surface rather than an evolving one.

See `ai/adr/ADR-009-contracts-before-planning.md` for the rationale.

---

## 2. What C2 introduced

### 2.1 Module boundary (C2.1 — issue #55)

A new Maven module `skadi-semantic` was added to the reactor (`pom.xml`).

| Property | Value |
| --- | --- |
| groupId | `org.iceforge` |
| artifactId | `skadi-semantic` |
| packaging | `jar` |
| Parent | `skadi-parent` (plain Java library — not Spring Boot) |
| Java release | 17 |
| Runtime dependencies | none |
| Test dependencies | `junit-jupiter`, `jackson-databind` (test scope only) |

The module has **no dependency on `skadi-sql-gateway`**. It does not start a Spring
context, open ports, or connect to any external system.

### 2.2 Core vocabulary records (C2.2 — issue #56)

All types live in `org.iceforge.skadi.semantic.contract`.

#### `SemanticContract`

The top-level governed dataset descriptor. Fields:

| Field | Type | Notes |
| --- | --- | --- |
| `name` | `String` | Unique identifier; used as the `ContractRegistry` lookup key |
| `version` | `SemanticContractVersion` | Semver string for the contract schema |
| `description` | `String` | Human-readable purpose |
| `entity` | `SemanticEntity` | Logical dataset name + physical binding |
| `measures` | `List<SemanticMeasure>` | Defensively copied; unmodifiable |
| `dimensions` | `List<SemanticDimension>` | Defensively copied; unmodifiable |
| `accessPolicy` | `SemanticAccessPolicy` | Descriptive access metadata |
| `cachePolicy` | `SemanticCachePolicy` | Descriptive cache hint |

All fields are required (null-checked in compact constructor). Name must not be blank.

#### `SemanticEntity`

Binds a logical name to a physical `SemanticEndpoint`. Fields: `name`, `description`,
`endpoint`, `ruleRefs` (defensively copied). Rule refs are pointers only — no rule
evaluation occurs here.

#### `SemanticEndpoint`

Physical `catalog.schema.table` binding. `fullyQualified()` returns the three-part
name. No connection is opened; this is a descriptor only.

#### `SemanticMeasure`

Named aggregation expression. `expression` is a `String` (e.g. `"SUM(pnl)"`) — it is
**never executed** by this record. The semantic compiler (C3) will convert it to a
fragment of Databricks SQL when a query is compiled. `type` is a `SemanticFieldType`.

#### `SemanticDimension`

Column binding. `column` maps the logical `name` to the physical column. `filterable`
and `groupable` flags are declarative metadata — the semantic compiler enforces them
when it becomes available in C3.

#### `SemanticRuleRef`

A reference to an external governance rule: a `ruleId` (e.g. `"BCBS239-01"`) and a
`description`. No rule logic; no evaluation. Used to attach BCBS239 lineage requirements,
data-quality check IDs, or other governance pointers to contracts and policies.

#### `SemanticContractVersion`

A semver string (`"1.0.0"`, `"2.1.0"`, etc.) that versions the contract definition
itself — not the underlying data. Breaking changes to a contract require a major version
bump.

#### `SemanticFieldType`

Enum: `STRING` | `INTEGER` | `LONG` | `DECIMAL` | `DATE` | `TIMESTAMP` | `BOOLEAN`.
Logical types only — no SQL type mapping is performed here.

### 2.3 Access and cache policy metadata (C2.3 — issue #57)

#### `SemanticAccessPolicy`

Descriptive metadata declaring which roles, principals, and governance rules apply to a
contract. It does **not** enforce access — it is a hint to a future semantic policy
enforcer (post-Lane C).

Key behaviours:
- All three lists (`allowedRoles`, `allowedPrincipals`, `ruleRefs`) are defensively
  copied and unmodifiable.
- `unrestricted()` factory returns an empty-list policy.
- `isEmpty()` returns true when all three lists are empty.
- An empty list means "no restriction declared" — the semantics of an empty access
  policy (allow-all vs. default-deny) are determined by the policy enforcer, not by
  this record.

#### `SemanticRoleRef`

A named role reference (e.g. `"risk_analyst"`). No role resolution; no membership lookup.

#### `SemanticPrincipalRef`

A typed principal: `SemanticPrincipalType` (USER / GROUP / SERVICE_ACCOUNT) + `name`.
No authentication; no directory lookup.

#### `SemanticCachePolicy`

A cache strategy hint. Fields: `strategy` (`SemanticCacheStrategy`), `ttlSeconds`
(`Long`, nullable for NONE and DATASET_VERSION), `ruleRefs`.

Key behaviours:
- `none()` factory: NONE strategy, null TTL.
- `ttl(long)` factory: TTL strategy, positive seconds enforced in compact constructor.
- `DATASET_VERSION` strategy is present as a future extension seam for Delta snapshot
  invalidation (gap L3, DQR-001). It has no effect in Lane C — implementations should
  treat it as NONE until the version resolver exists.
- The cache layer (`skadi-server`) decides whether to honour the TTL hint; this record
  does not write to any cache.

### 2.4 ContractRegistry interface (C2.4 — issue #58)

`ContractRegistry` is a **plain Java interface** in `org.iceforge.skadi.semantic.registry`.
It is not a Spring service, not a singleton, and does not load files.

| Method | Behaviour |
| --- | --- |
| `register(SemanticContract)` | Adds under `contract.name()`; throws `DuplicateContractException` on collision |
| `findByName(String)` | Returns `Optional<SemanticContract>`; empty if not found |
| `list()` | Unmodifiable snapshot |
| `forPrincipal(String)` | Abstract seam for Lane E; stub returns `list()` until access policy enforcement lands |
| `contains(String)` | Default; delegates to `findByName` |
| `remove(String)` | Default; throws `UnsupportedOperationException`; mutable implementations override |

`DuplicateContractException` is an unchecked exception; it carries `contractName()`.

`InMemoryContractRegistry` lives in `src/test/java`. It is a mutable,
insertion-order-preserving implementation for unit tests. **It must not appear in
production code or in `src/main/java`.**

### 2.5 JSON serialization (C2.5 — issue #59)

Jackson 2.19.2 (via Spring Boot BOM) is added as a **test-scope** dependency.
Jackson 2.14+ supports Java records natively through `RecordComponent.getName()` — no
`@JsonProperty` annotations on production records and no `--parameters` compiler flag
are needed.

The `ObjectMapper` in tests is configured with `AUTO_DETECT_IS_GETTERS` disabled to
prevent `SemanticAccessPolicy.isEmpty()` from appearing as a spurious `"empty"` field
in serialised JSON.

Fixture: `src/test/resources/fixtures/sample-contract.json` — a complete
`SemanticContract` with 2 measures, 3 dimensions, 2 roles, 2 principals, and a TTL
cache policy of 7200 seconds.

Test class: `SemanticContractJsonTest` — 23 offline, deterministic tests covering
round-trips, enum serialisation, fixture loading, structural JSON equality, and
unmodifiable list enforcement after deserialisation.

---

## 3. What does not exist in C2

| Missing capability | Tracking |
| --- | --- |
| `SemanticQuery` record | C3 |
| `SemanticCompiler` interface | C3 |
| `CacheContract` / `CacheIdentity` | C4 |
| `SemanticExecutor` interface | C5 |
| Contract file loader (YAML/JSON/any) | DQR-001 → post-Lane C |
| Runtime contract registry population | Post-Lane C |
| Access policy enforcement | Post-Lane C semantic policy enforcer |
| SQL generation or dialect compilation | Post-Lane C semantic compiler implementation |
| Semantic planner / rule engine | Post-Lane C (ADR-009) |
| UI bricks | Lane D |
| AI intent resolution / LLM calls | Lane E |

---

## 4. How C3 should extend C2

C3 (issue #41) adds query contract and output-shape metadata to `skadi-semantic`.

### What to add

- `SemanticQuery` record — captures a caller's intent:
  `datasetName`, `List<String> measureNames`, `List<String> dimensionNames`,
  `List<SemanticFilter> filters`. Field names reference `SemanticContract.name()`,
  `SemanticMeasure.name()`, and `SemanticDimension.name()`.
- `OutputShape` record — describes the expected result shape:
  column names, `SemanticFieldType` per column, optional row count hint.
- `SemanticCompiler` interface — `compile(SemanticQuery, PlanHints): String`. Returns
  Databricks SQL as a `String` descriptor. The `PlanHints` parameter is an empty record
  in C3 (extension seam for future planner). The stub implementation returns fixture SQL.

### What NOT to add in C3

- Do not invoke `SqlDialectBridge` — the compiler stub returns a hardcoded string.
- Do not build a query planner or rule evaluator.
- Do not add Spring beans.
- Do not validate `SemanticQuery` field names against the contract (that is the
  semantic governor's job, post-Lane C).

### How C3 uses C2 types

`SemanticQuery` references measure and dimension names that must exist in a
`SemanticContract`. The lookup is performed by `ContractRegistry.findByName()` —
the registry interface defined in C2.4 is the dependency.

---

## 5. How C4 should extend C2

C4 (issue #42) adds cache contract boundary types.

### What to add

- `CacheContract` record — wraps `SemanticCachePolicy` from C2 into a boundary type
  passed from the semantic layer outward to `skadi-server`. Fields: `ttl`, `strategy`,
  optionally `datasetVersionToken` (null in Lane C — see L3).
- `CacheIdentity` record — the logical cache key: normalised SQL + principal name +
  optional dataset version token.

### What NOT to add in C4

- Do not change `skadi-server` cache internals.
- Do not implement the dataset version resolver (`DATASET_VERSION` strategy).
- Do not write to or read from any cache.

### How C4 uses C2 types

`CacheContract` reads `SemanticCachePolicy.strategy()` and `SemanticCachePolicy.ttlSeconds()`
from `SemanticContract.cachePolicy()`. It translates the policy hint into a boundary-crossing
type that `skadi-server` will eventually receive when C5 activates the HTTP call.

---

## 6. How C5 should extend C2

C5 (issue #43) adds the service interface for semantic-aware execution.

### What to add

- `SemanticExecutor` interface — `execute(SemanticQuery, String principal): ArrowResult`.
- `StubSemanticExecutor` — returns fixture Arrow data from a test resource file.
- `SkadiserverSemanticExecutor` skeleton — a class body with a
  `// TODO: activate HTTP call to POST /api/v1/queries` comment and no real HTTP code.
- Wire `StubSemanticExecutor` as the active bean behind a `skadi.semantic.executor=stub`
  config flag.

### What NOT to add in C5

- Do not implement the live HTTP call to `skadi-server` (tracked by DQR-002).
- Do not add REST endpoints to `skadi-semantic`.
- Do not connect to Databricks directly.

### How C5 uses C2 types

`SemanticExecutor.execute()` will call `ContractRegistry.findByName()` to resolve the
contract, pass `SemanticCachePolicy` to a `CacheContract` builder (C4), and delegate to
`SkadiserverSemanticExecutor` (or the stub). All dependency is through the `ContractRegistry`
interface — no coupling to any specific implementation.

---

## 7. Architecture references

| Document | Relevance to C2 |
| --- | --- |
| `ai/adr/ADR-008-lane-c-scope.md` | Lane C scope: records and interfaces only, no planner/runtime |
| `ai/adr/ADR-009-contracts-before-planning.md` | Why C2 precedes C3 (SemanticCompiler) and the planner |
| `ai/adr/ADR-010-cache-positioning.md` | Cache stays below consumers; `SemanticCachePolicy` is a hint |
| `ai/dqr/DQR-001-contract-definition-format.md` | Contract storage format — open; YAML is the leading candidate |
| `ai/dqr/DQR-002-semantic-execution-delegation.md` | How C5's executor delegates to `skadi-server` |
| `ai/architecture/platform-boundary-model.md` | Full platform diagram; Semantic Contract Layer section |
