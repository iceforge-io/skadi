# skadi-semantic

Lane C module — semantic contract skeletons (C2 + C3 + C4 + C5 complete).

Plain Java library. No Spring Boot, no SQL execution, no YAML loading,
no REST endpoints, no Databricks calls.

---

## What this module is

A contract-only boundary that defines the **semantic vocabulary** for the Skadi
platform. It contains immutable records and interfaces that describe datasets,
measures, dimensions, access policies, and cache policies.

Lane D (UI Brick Runtime) and Lane E (AI Chat Buddy) depend on the types defined
here after Lane C completes. C3/C4/C5 extend these types without changing them.

## What this module is NOT

| Not present | Reason |
| --- | --- |
| Semantic planner / rule engine | Out of scope for Lane C — see ADR-008 |
| SQL generation or execution | Delegated to `skadi-server` via a future executor interface |
| YAML / JSON Schema loading | Format decision deferred — see DQR-001 |
| Runtime contract registry population | Deferred to post-Lane C |
| Spring beans or `@Component` annotations | Plain library; no Spring context |
| REST endpoints or controllers | No runtime behaviour |
| Databricks, S3, or Tableau integration | No external system calls |
| Entitlement enforcement | `SemanticAccessPolicy` is descriptive metadata only |
| Cache behavior changes | `SemanticCachePolicy` is a hint, not a cache command |
| UI bricks or AI chatbot | Future lanes D and E |

---

## Package layout

```
org.iceforge.skadi.semantic            module root (package-info only)
org.iceforge.skadi.semantic.contract   vocabulary records and enums (C2)
org.iceforge.skadi.semantic.registry   ContractRegistry interface (C2)
org.iceforge.skadi.semantic.query      query contract and output-shape metadata (C3)
org.iceforge.skadi.semantic.cache      cache boundary contracts (C4)
org.iceforge.skadi.semantic.service    service boundary interfaces and context records (C5)
```

---

## Records — core vocabulary (`contract` package, C2.2)

| Type | Purpose |
| --- | --- |
| `SemanticContract` | Top-level contract: name, version, entity, measures, dimensions, access policy, cache policy |
| `SemanticEntity` | Logical dataset name bound to a `SemanticEndpoint` |
| `SemanticEndpoint` | Physical `catalog.schema.table` binding; `fullyQualified()` helper |
| `SemanticMeasure` | Named aggregation expression descriptor (e.g. `SUM(pnl)`); expression is a `String`, never executed |
| `SemanticDimension` | Column binding with `filterable` / `groupable` flags |
| `SemanticRuleRef` | Pointer to an external governance rule; no rule logic here |
| `SemanticContractVersion` | Semver wrapper for contract schema versioning |
| `SemanticFieldType` | Enum: `STRING` `INTEGER` `LONG` `DECIMAL` `DATE` `TIMESTAMP` `BOOLEAN` |

All records validate required fields in their compact constructors (`Objects.requireNonNull`,
blank checks). All `List` fields are defensively copied with `List.copyOf()` — returned lists
are always unmodifiable.

## Records — access and cache policy metadata (`contract` package, C2.3)

| Type | Purpose |
| --- | --- |
| `SemanticAccessPolicy` | Descriptive metadata: allowed roles, principals, rule refs; `unrestricted()` factory; `isEmpty()` helper |
| `SemanticRoleRef` | Named role reference; no role resolution or enforcement |
| `SemanticPrincipalRef` | Typed principal reference (type + name); no authentication |
| `SemanticPrincipalType` | Enum: `USER` `GROUP` `SERVICE_ACCOUNT` |
| `SemanticCachePolicy` | Cache strategy hint + optional TTL; `none()` and `ttl(n)` factories; validates TTL strategy requires positive seconds |
| `SemanticCacheStrategy` | Enum: `NONE` `TTL` `DATASET_VERSION` (DATASET_VERSION is a future seam — see ADR-010 and gap L3) |

## ContractRegistry (`registry` package, C2.4)

`ContractRegistry` is a **plain Java interface**. It is not a Spring service.

```
register(SemanticContract)        — adds a contract; throws DuplicateContractException on collision
findByName(String)                — returns Optional<SemanticContract>
list()                            — unmodifiable snapshot of all registered contracts
forPrincipal(String principalName)— returns accessible contracts (stub: returns list() in Lane C)
contains(String)                  — default method; delegates to findByName
remove(String)                    — default throws UnsupportedOperationException
```

`DuplicateContractException` is an unchecked exception; carries `contractName()`.

`InMemoryContractRegistry` lives in `src/test/java` — mutable, insertion-order preserving,
for unit tests only. It must not be used in production code.

---

## JSON testing conventions (C2.5)

- Test fixture: `src/test/resources/fixtures/sample-contract.json`
- Demo spike fixtures (issue #122 / epic #121):
  - `src/test/resources/fixtures/demo-contract-risk-sensitivity-exposure-grid.json`
  - `src/test/resources/fixtures/demo-contract-risk-sensitivity-historical-timeseries.json`
  - These are governed market-risk sensitivity contracts used by the plain-English demo spike.
    They are test-only fixtures — no production table names, no query execution, no SQL gateway changes.
    Source view config key wiring is a follow-up TODO documented in each fixture's entity description.
- `jackson-databind` is a **test-scope** dependency only (version managed by Spring Boot BOM).
- The `ObjectMapper` in tests is configured with `AUTO_DETECT_IS_GETTERS` disabled.
  Jackson 2.14+ deserialises records via `RecordComponent.getName()` — no `@JsonProperty`
  annotations or `--parameters` compiler flag are required.
- No YAML tests. No JSON Schema validation.
- Test class: `SemanticContractJsonTest` (23 tests, all offline).

---

## Records — query contract and output-shape metadata (`query` package, C3)

| Type | Purpose |
| --- | --- |
| `SemanticQueryContract` | Named, versioned query contract: name, source contract, version, output shape, references |
| `SemanticOutputShape` | Ordered column list with optional row-count hint; `findColumn(name)` helper |
| `SemanticOutputColumn` | Single output column: name, display name, field type, nullable, role, format hint, references |
| `SemanticOutputRole` | Enum: `MEASURE` `DIMENSION` `TIMESTAMP` `IDENTIFIER` `LABEL` `DERIVED` |
| `SemanticFormatHint` | Display-only hints: pattern, unit, currency, precision, scale (all nullable) |
| `SemanticReference` | Lightweight pointer: source / type / id / name — no resolution, no external calls |

`SemanticFieldType` and `SemanticContractVersion` from C2 are reused here.
`SemanticFormatHint` is nullable on `SemanticOutputColumn`; `rowCountHint` is nullable on `SemanticOutputShape`.
All `List` fields are defensively copied.

---

## What does not exist yet

| Missing capability | Where it belongs |
| --- | --- |
| Contract file loader (YAML/JSON/other) | Post-DQR-001 |
| Runtime registry population | Post-Lane C |
| Access policy enforcement | Post-Lane C semantic policy enforcer |
| SQL generation or semantic compiler implementation | Post-Lane C |
| Semantic planner / rule engine | Post-Lane C |
| UI bricks | Lane D |
| AI Chat / intent resolution | Lane E |

---

---

## Lane C story sequence (C2 + C3 complete)

| Story | Issue | Status | Deliverable |
| --- | --- | --- | --- |
| C2.1 | #55 | ✅ | `skadi-semantic` Maven module and package skeleton |
| C2.2 | #56 | ✅ | Core semantic contract records |
| C2.3 | #57 | ✅ | Access policy and cache policy skeletons |
| C2.4 | #58 | ✅ | `ContractRegistry` interface and `DuplicateContractException` |
| C2.5 | #59 | ✅ | JSON serialization fixture and tests |
| C2.6 | #60 | ✅ | Documentation and implementation notes (this file) |
| C3   | #41 | ✅ | Query contract and output-shape metadata (`query` package) |
| C4   | #42 | ✅ | Cache boundary contracts (`cache` package) |
| C5   | #43 | ✅ | Service boundary interfaces and context records (`service` package) |

---

## Architecture references

| Document | Contents |
| --- | --- |
| `ai/lane-c/c2-semantic-contract-skeletons.md` | Detailed C2 implementation notes and C3/C4/C5 guidance |
| `ai/adr/ADR-008-lane-c-scope.md` | Lane C scope boundary — what is and is not built |
| `ai/adr/ADR-009-contracts-before-planning.md` | Why contracts precede the semantic planner |
| `ai/adr/ADR-010-cache-positioning.md` | Cache positioned below all higher-level consumers |
| `ai/dqr/DQR-001-contract-definition-format.md` | Open question: contract storage format (YAML vs JSON vs other) |
| `ai/dqr/DQR-002-semantic-execution-delegation.md` | Open question: how skadi-semantic delegates to skadi-server |
| `ai/architecture/platform-boundary-model.md` | Full platform boundary diagram and component tables |
