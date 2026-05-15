# Lane D — Contract Definition Format

**Decision:** JSON canonical runtime format (ADR-011, Accepted 2026-05-14)
**Story:** D2 / skadi#63
**Status:** Resolved

---

## Summary

| Layer | Format | Lane |
|---|---|---|
| In-memory model | Java records (`SemanticContract`, `SemanticEntity`, etc.) | Lane C — unchanged |
| Runtime contract files | JSON (`.json`, one per contract) | Lane D (D3 loader) |
| Human-authoring layer | YAML — deferred | post-Lane D |
| Validation schema | JSON Schema — deferred to D6 | Lane D D6 |

---

## What this means for each Lane D story

**D3 (file-backed loader):**
- Reads `*.json` files from a configurable directory
- Deserializes each file into a `SemanticContract` via Jackson (`ObjectMapper`)
- Promotes `jackson-databind` from test scope to compile scope in `skadi-semantic/pom.xml`
- Does NOT add a YAML parser dependency
- On `JsonProcessingException`, reports the file path and Jackson error message

**D4 (registry population):**
- Calls `ContractRegistry.register()` for each `SemanticContract` returned by the loader
- No format dependency — population works on Java records only

**D5 (resolver):**
- Looks up `SemanticContract` by name from the registry
- No format dependency

**D6 (validation):**
- Adds `ContractValidator` with structural checks beyond Jackson parsing errors
- May add JSON Schema validation at this point — scoped and decided in D6

**D7 (read-only metadata endpoint):**
- Returns contract metadata from the registry as JSON (Spring MVC serializes the records)
- The wire format for the API response is also JSON; no separate format decision required

---

## Canonical example

The existing Lane C fixture is the canonical contract file example:

```
skadi-semantic/src/test/resources/fixtures/sample-contract.json
```

This file represents a complete `SemanticContract` for the `mxl_risk` dataset with:
- Two measures (`pnl`, `delta_risk`)
- Three dimensions (`cob_date`, `book`, `desk`)
- An access policy with allowed roles and principals
- A TTL cache policy (7200 seconds)

D3 tests should load this file and assert the resulting `SemanticContract` matches the
expected field values. This double-registers the fixture as a loader conformance test.

---

## YAML — deferred, not rejected

ADR-005 (Proposed, not Accepted) named YAML as the leading candidate for contract authoring.
ADR-011 defers YAML to a post-Lane D story. The rationale:

1. Lane C JSON fixtures already prove the Jackson round-trip for all contract types.
2. YAML adds a parser dependency that requires a separate decision (SnakeYAML vs.
   `jackson-dataformat-yaml` vs. another library).
3. JSON is machine-editable and AI-agent-friendly without the YAML ambiguity risks
   (`ON`/`OFF` as boolean, indentation sensitivity, anchors/aliases).

A future story can add a YAML → Java record conversion step that feeds the same D3 loader.
That story updates ADR-011 to Amended and records the YAML parser choice.

---

## JSON Schema validation — deferred to D6

D2/D3 do not add JSON Schema validation. Jackson's `ObjectMapper` is the only validation
layer in D3:
- Unknown fields: raise `UnrecognizedFieldException` if strict mode is enabled
- Missing required fields: raise `MismatchedInputException`
- Type mismatches: raise `InvalidDefinitionException`

D6 adds `ContractValidator` with higher-level structural checks:
- Duplicate contract names across files
- Duplicate measure/dimension names within a contract
- Query contracts referencing a semantic contract that is not loaded
- Unsupported cache policy combinations

JSON Schema validation (a separate schema file validated against each contract JSON) is an
optional addition in D6 or later — not required for Lane D to succeed.

---

## File layout (Lane D target)

```
skadi-semantic/
└── contracts/                          ← configurable path (D3)
    ├── mxl_risk.json
    ├── credit_risk.json
    └── ...

skadi-semantic/src/test/resources/
└── fixtures/
    ├── sample-contract.json            ← Lane C; canonical example; D3 loader test
    ├── sample-query-contract.json      ← Lane C; D5/D7 tests
    └── sample-cache-boundary.json      ← Lane C; cache boundary tests
```

The `contracts/` directory under `skadi-semantic/` is the runtime location. Classpath and
filesystem paths are both supported — configuration property `skadi.semantic.contracts.path`.
The test fixtures under `src/test/resources/fixtures/` remain as Lane C test assets.
