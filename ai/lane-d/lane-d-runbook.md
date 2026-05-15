# Lane D Runbook — Contract Loading, Resolution, and Runtime Activation

> Status: Complete ✅ — all 8 issues closed, Lane D merged to `main`
> Last updated: 2026-05-15

---

## What Lane D did

Lane D activated the Lane C semantic contract boundary by making contracts loadable,
validateable, and resolvable at runtime. It also exposed a minimal read-only HTTP
metadata surface in `skadi-server`.

No semantic planner, SQL generator, rule engine, entitlement engine, or Databricks
execution path was built. `SkadiServerQueryExecutionService` remains a skeleton.

---

## Module impact

| Module | Changed? | What changed |
|---|---|---|
| `skadi-semantic` | ✅ Yes | Added `loader`, `validation` packages; populated `registry` package with `LoadedContractRegistry`, `ContractRegistryPopulator`, `ContractRegistryPopulationResult`, `ContractRegistryPopulationException`; added `RegistrySemanticContractResolver`; promoted `jackson-databind` to compile scope |
| `skadi-server` | ✅ Yes | Added `skadi-semantic` dependency; added `org.iceforge.skadi.semantic` package with `SemanticContractProperties`, `SemanticContractConfiguration`, `SemanticContractMetadataController` |
| `skadi-sql-gateway` | ❌ No changes | |
| `ai/` | ✅ Yes | ADR-011, DQR-001 resolved, `ai/lane-d/` docs |

---

## Issue-to-commit map

| Issue | Story | Commit | Description |
|---|---|---|---|
| #61 | Epic | — | Lane D epic |
| #62 | D1 | `4606eff` | Activation boundary document |
| #63 | D2 | `bc081e8` | JSON canonical format decision; ADR-011; DQR-001 resolved |
| #64 | D3 | `4360f96` | `ContractLoader`, `JsonContractLoader`, `ContractLoadException` |
| #67 | D6 | `3339075` | `ContractValidator`, `SemanticContractValidator`, validation result types |
| #65 | D4 | `97eea43` | `ContractRegistryPopulator`, `LoadedContractRegistry`, population result types |
| #66 | D5 | `2f543ab` | `RegistrySemanticContractResolver` |
| #68 | D7 | `c0506a8` | `SemanticContractMetadataController`, `SemanticContractConfiguration`, `SemanticContractProperties` |
| #69 | D8 | — | Dev-status and this runbook |

Execution order was D1 → D2 → D3 → D6 → D4 → D5 → D7 → D8 (validation before population, per D1 guidance).

---

## Build and test

```bash
# skadi-semantic only (fastest — no Spring Boot startup)
mvn verify -pl skadi-semantic -am

# skadi-semantic + skadi-server (includes controller tests with Spring Boot)
mvn verify -pl skadi-semantic,skadi-server -am

# Full project build
mvn verify
```

Expected test counts after D8:

| Module | Tests |
|---|---|
| `skadi-semantic` | 378 |
| `skadi-server` | 116 |
| Combined | 494 |

`skadi-sql-gateway` test counts are unchanged from Lane B (232).

---

## Contract file format

**JSON is canonical for Lane D** (ADR-011, Accepted 2026-05-14).
YAML is deferred — not rejected. See `ai/lane-d/contract-format.md` for details.

Canonical example:

```
skadi-semantic/src/test/resources/fixtures/sample-contract.json
```

One JSON file per `SemanticContract`. File naming: `<contract-name>.json`.

---

## Validation pipeline

```
JSON files
    ↓  JsonContractLoader.loadAll(paths)
List<SemanticContract>
    ↓  SemanticContractValidator.validateAll(contracts)
ContractValidationResult
    ↓  if hasErrors() → abort; return null registry
    ↓  if warningsOnly → proceed with warnings logged
LoadedContractRegistry (read-only)
    ↓  RegistrySemanticContractResolver.of(registry)
SemanticContractResolver
```

### ERROR vs WARNING

| Severity | Behaviour |
|---|---|
| `ERROR` | Aborts registry population; `ContractRegistryPopulator.populate()` throws; `populateWithResult()` returns `registry=null` |
| `WARNING` | Registry is populated; warnings available in `ContractRegistryPopulationResult.validationResult()` |

### Issue codes

| Code | Severity | Trigger |
|---|---|---|
| `DUPLICATE_CONTRACT_NAME` | ERROR | Two contracts share a name in the validated list |
| `CONTRACT_DUPLICATE_MEASURE` | ERROR | Two measures share a name within one contract |
| `CONTRACT_DUPLICATE_DIMENSION` | ERROR | Two dimensions share a name within one contract |
| `CONTRACT_NO_MEASURES` | WARNING | Contract defines no measures |
| `CONTRACT_NO_DIMENSIONS` | WARNING | Contract defines no dimensions |
| `CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED` | WARNING | Cache strategy `DATASET_VERSION` declared (future feature) |
| `QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN` | ERROR | Two output columns share a name in a `SemanticQueryContract` |
| `QUERY_CONTRACT_UNRESOLVED_SOURCE` | ERROR | `SemanticQueryContract.sourceContract` not in the provided semantic contract list |

---

## Read-only metadata endpoint

### Configuration

```yaml
skadi:
  semantic:
    contracts:
      enabled: true          # default: false — server starts normally without this
      locations:
        - /etc/skadi/contracts/mxl_risk.json
        - /etc/skadi/contracts/credit.json
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/semantic/contracts` | List loaded contracts (summary: name, version, measureCount, dimensionCount) |
| `GET` | `/api/semantic/contracts/validation` | Validation status: valid, errorCount, warningCount, issues list |
| `GET` | `/api/semantic/contracts/{name}` | Full contract detail; `404` if not found |

### Behavior when disabled

- `skadi.semantic.contracts.enabled=false` (default): registry is empty; `GET /api/semantic/contracts` returns `{"enabled": false, "contractCount": 0, "contracts": []}`.
- Missing or invalid contract files: logged as errors; empty registry returned; server startup continues normally.
- Access policy: **not enforced** — all contracts visible to any caller. Enforcement deferred to post-Lane D.

### Sample responses

**`GET /api/semantic/contracts`:**
```json
{
  "enabled": true,
  "contractCount": 1,
  "contracts": [
    {"name": "mxl_risk", "version": "1.0.0", "description": "...", "measureCount": 2, "dimensionCount": 3}
  ]
}
```

**`GET /api/semantic/contracts/mxl_risk`:**
```json
{
  "name": "mxl_risk", "version": "1.0.0", "description": "...",
  "measureCount": 2, "dimensionCount": 3,
  "cacheStrategy": "TTL", "ttlSeconds": 7200
}
```

**`GET /api/semantic/contracts/validation`:**
```json
{"valid": true, "errorCount": 0, "warningCount": 0, "issues": []}
```

---

## Contract resolution

`RegistrySemanticContractResolver.of(registry)` resolves by name:

```java
var resolver = RegistrySemanticContractResolver.of(registry);
var req = new SemanticResolutionRequest("mxl_risk", ExecutionContext.of("alice"));
SemanticResolutionResult result = resolver.resolve(req);
if (result.resolved()) {
    SemanticContract c = result.contract();
} else {
    // result.errorMessage() contains contract name + principal name for diagnostics
}
```

- Found → `SemanticResolutionResult.found(contract)`, `resolved()=true`
- Not found → `SemanticResolutionResult.notFound(message)`, `resolved()=false`, never throws
- Access policy → **not enforced**; all principals see all contracts

---

## Open design questions (DQRs)

| DQR | Status | Impact on next lane |
|---|---|---|
| DQR-002 | Open | Blocks `SkadiServerQueryExecutionService` activation — semantic execution topology |
| DQR-003 | Open | Lineage/Market Risk Brain integration seams |

DQR-001 was resolved in D2 (JSON canonical).

---

## What remains unchanged

- `SkadiServerQueryExecutionService` — still throws `UnsupportedOperationException`. Do not activate without resolving DQR-002.
- `skadi-sql-gateway` — no changes in Lane D. Direct Databricks JDBC path unchanged.
- `SemanticAccessPolicy` — records exist but are not evaluated at runtime in Lane D.

---

## Validation checklist

Run after each story or after any change to `skadi-semantic` or `skadi-server`:

```bash
# 1. No source changes to skadi-sql-gateway
git diff --name-only HEAD | grep 'skadi-sql-gateway/src/' && echo "FAIL: gateway changed" || echo "OK"

# 2. Whitespace clean
git diff --check

# 3. Core semantic tests
mvn verify -pl skadi-semantic -am

# 4. Full affected modules
mvn verify -pl skadi-semantic,skadi-server -am

# 5. Skeleton not activated
grep -q 'UnsupportedOperationException' \
  skadi-semantic/src/main/java/org/iceforge/skadi/semantic/service/SkadiServerQueryExecutionService.java \
  && echo "OK — skeleton intact" || echo "FAIL: skeleton modified"
```

---

## Next lane recommendations

The following are candidate directions for the next lane. The next lane should be
chosen deliberately — no commitment is implied by this runbook.

### Candidate: Lane E-a — Semantic execution activation

**Trigger:** Resolve DQR-002 (execution delegation topology).

**Scope:** Replace `SkadiServerQueryExecutionService` skeleton with a real HTTP call to
`POST /api/v1/queries` on `skadi-server`. This activates the full semantic query path:
contract resolution → `SkadiServerQueryExecutionService` → `skadi-server` → Databricks.

**Prerequisite:** DQR-002 must be resolved and recorded in an ADR before writing any HTTP
client code.

### Candidate: Lane E-b — Contract authoring ergonomics

**Trigger:** Data engineers need a more human-friendly authoring format.

**Scope:** Add a YAML → JSON pre-processing step (see ADR-011, section 8, Q3). The D3
`ContractLoader` is designed to be swappable; a YAML loader can feed the same pipeline.
This requires a YAML parser dependency decision (SnakeYAML vs. `jackson-dataformat-yaml`).

### Guardrail for either candidate

Do not start either lane until:
1. The relevant DQR is resolved and recorded in an ADR.
2. `SkadiServerQueryExecutionService` still throws `UnsupportedOperationException` at the
   start of the lane.
3. A D1-equivalent activation boundary document exists for the new lane.

---

## Agent instructions for Lane E

1. Read `ai/lane-d/lane-d-activation-boundary.md` and this runbook before implementing anything.
2. Do not activate `SkadiServerQueryExecutionService` without resolving DQR-002.
3. Do not add entitlement enforcement without a dedicated ADR and lane scope document.
4. Do not generate SQL from contract metadata without a planner-boundary document.
5. Keep offline tests for all new `skadi-semantic` components.
6. Run `git diff --check` before every commit.
7. One commit per story, tagged `skadi#<issue>`.
