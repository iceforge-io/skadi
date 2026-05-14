# Lane C Runbook — Contracts, Skeletons, and Boundaries

> Status: Complete ✅ — all 13 issues closed, `skadi-semantic` module merged to `main`
> Last updated: 2026-05-14

---

## What Lane C did

Lane C created the `skadi-semantic` Maven module: a plain Java library of records, interfaces, and
no-op/skeleton implementations that define the platform's semantic layer contracts. No production
behavior was changed. No Spring beans were introduced. No execution wiring was activated.

---

## Module layout

```
skadi-semantic/
├── pom.xml                          # inherits skadi-parent; jackson test-scope only
└── src/
    ├── main/java/org/iceforge/skadi/semantic/
    │   ├── contract/                # SemanticContract and supporting records
    │   ├── cache/                   # CacheIdentity, CacheContract, cache boundary types
    │   └── service/                 # service interfaces + no-op/skeleton impls
    └── test/java/org/iceforge/skadi/semantic/
        ├── ContractCompositionTest  # cross-package and cross-lane boundary tests
        ├── contract/                # SemanticContractTest, ContractRegistryTest
        ├── cache/                   # CacheIdentityTest, CacheBoundaryJsonTest
        └── service/                 # ServiceBoundaryTest
```

---

## Issue-to-commit map

| Issue | Story | Commit | Description |
|---|---|---|---|
| #39 | C1 | `61aa0a3` (rev `7297aac`) | Platform boundary model architecture doc |
| #44 | C6 | `3e6c76e` + `0e2c4aa` | ADR-008/009/010, DQR-001/002/003 |
| #55 | C2.1 | `b32d59f` | `skadi-semantic` Maven module + package scaffold |
| #49 | C2.2 | `dc02035` | Core semantic contract records |
| #50 | C2.3 | `8a1833c` | SemanticAccessPolicy, SemanticCachePolicy |
| #51 | C2.4 | `51a21fe` | ContractRegistry interface |
| #52 | C2.5 | `95bf0f7` | JSON serialization fixtures and tests |
| #53 | C2.6 | `5f45635` | C2 implementation notes |
| #41 | C3 | `7cec95e` | SemanticQueryContract, SemanticOutputShape, SemanticOutputColumn |
| #42 | C4 | `e2377de` | CacheIdentity, CacheContract, CacheLookupResult, CacheWriteResult |
| #43 | C5 | `d462813` | Service interfaces; ExecutionContext/Request/Result; no-op/skeleton impls |
| #45 | C7 | `d41868b` | ContractCompositionTest (25 tests; package isolation, JSON fixtures) |
| #46 | C8 | — | Dev-status, this runbook |

---

## Build and test

```bash
# Build skadi-semantic and its transitive dependencies only
mvn verify -pl skadi-semantic -am

# Run skadi-semantic tests only (fastest feedback)
mvn test -pl skadi-semantic

# Full project build (ensures no regressions in gateway/server)
mvn verify
```

Expected: **258 tests passing** across all modules (`skadi-sql-gateway` and `skadi-server` counts
unchanged from Lane B).

---

## Key design decisions

### Java records throughout

All domain types are immutable Java 17 records. Compact constructors perform all validation.
`List` fields are defensively copied via `List.copyOf()`.

### No Spring beans

`skadi-semantic` is a plain jar. No `@Component`, `@Service`, or `@Configuration` annotations exist.
The module is intended to be a dependency of future activation modules (Lane D+), not a Spring
application itself.

### Jackson test-scope only

Jackson (`jackson-databind`, `jackson-datatype-jsr310`) is `<scope>test</scope>`. The main
production code has zero serialization dependencies. Tests use `ObjectMapper` with:
- `JavaTimeModule` registered (for `Instant`)
- `SerializationFeature.WRITE_DATES_AS_TIMESTAMPS` disabled
- `MapperFeature.AUTO_DETECT_IS_GETTERS` disabled (prevents `isEmpty()` on `SemanticAccessPolicy`
  being misread as a JSON property)

### SHA-256 cache fingerprint

`CacheIdentity.fingerprint()` produces a 64-char lowercase hex SHA-256 over
`"sql={}\nprincipal={}\nversion={}\n"`. This is a stable, collision-resistant key safe to use as a
filename or distributed cache key without percent-encoding.

### No-op vs skeleton distinction

- **No-op** (`NoOpCacheLookupService`, `NoOpLineageContextProvider`): implements the interface with
  safe, do-nothing behaviour (ABSENT lookups, empty lineage lists). Suitable for tests and early
  wiring where the real implementation is not yet needed.
- **Skeleton** (`SkadiServerQueryExecutionService`): throws `UnsupportedOperationException` on every
  call. Marks an integration point that is explicitly NOT ready. Tests must not inject this class
  unless they are asserting the throw.

---

## Open design questions (DQRs)

These were raised during Lane C and remain unresolved. They gate future activation work.

| DQR | Question | File |
|---|---|---|
| DQR-001 | Contract definition format (YAML vs JSON) | `ai/dqr/DQR-001-contract-definition-format.md` |
| DQR-002 | Semantic execution delegation topology (in-process vs HTTP) | `ai/dqr/DQR-002-semantic-execution-delegation.md` |
| DQR-003 | Lineage and Market Risk Brain integration seams | `ai/dqr/DQR-003-lineage-market-risk-brain-seams.md` |

---

## Activation guidance (Lane D+)

Lane C deliberately stopped short of wiring any execution. The following steps are needed to
activate the semantic layer:

1. **Resolve DQR-001** — decide contract format; implement a `ContractLoader` (YAML/JSON → `SemanticContract`).
2. **Resolve DQR-002** — decide execution topology (in-process JDBC vs HTTP to `skadi-server`).
3. **Activate `SkadiServerQueryExecutionService`** — replace `UnsupportedOperationException` body
   with the real HTTP call to `POST /api/v1/queries` on `skadi-server`. See issue `skadi#43`.
4. **Wire `CacheLookupService`** — connect the existing gateway cache to the `CacheLookupService`
   interface so the semantic layer can drive cache decisions.
5. **Register contracts** — implement a `ContractRegistry` bean backed by the chosen loader; expose
   it via an actuator or REST endpoint for inspection.
6. **Route pgwire queries** — modify `PgWireSession` to resolve a `SemanticQueryContract` when a
   named contract is requested, then use `QueryExecutionService` to execute.

None of these steps are in scope for Lane C. Do not start them until the relevant DQRs are resolved.

---

## Technical debt items relevant to activation

| ID | Item | Blocking? |
|---|---|---|
| L10 | Gateway embeds own JDBC pool; `SkadiServerQueryExecutionService` is a skeleton | Blocks full semantic routing |
| L2 | Dialect bridge bypassed on zero-param cache path 1 | Medium risk when semantic SQL is routed |
| L3 | Dataset version absent from cache keys — stale results possible | Medium risk when contracts drive cache |
| L9/L22 | STARTTLS not implemented — use stunnel/Envoy for production TLS | Pre-production critical |

See the full Technical Debt Register in `ai/dev-status.md`.
