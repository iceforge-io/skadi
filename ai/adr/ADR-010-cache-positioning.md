# ADR-010: Cache Positioning — Between Databricks and All Consumers

**Status:** Accepted
**Date:** 2026-05-14
**Owners:** engineering, architecture
**Related ADRs:** ADR-004, ADR-005, ADR-008, ADR-009
**Related Issues:** #38 (Lane C epic), #44 (C6 ADRs/DQRs)
**Supersedes:** —

---

## 1. Context

### Problem

As Skadi acquires a semantic layer (ADR-004) alongside the existing raw SQL path, there
are two natural positions for caching:

1. **Above the semantic layer** — cache the semantic query (`dataset + metrics + dims`)
   so the compiler and execution layer are bypassed entirely on cache hit.
2. **Below the semantic layer** — cache the compiled SQL result so the semantic layer
   issues a compilation on every request but skips execution on a hit.

Option 1 requires the cache to understand semantic query objects (`SemanticQuery`,
`ContractRegistry`, principal identity). Option 2 means the cache only sees normalised SQL
strings — the same interface it presents to the raw SQL path today.

Without an explicit decision, Lane D and Lane E will make conflicting assumptions about
where cache invalidation belongs, leading to inconsistent cache key design and possible
stale-result bugs.

### Background

- `skadi-server` owns the current cache: an in-process TTL map keyed on normalised SQL
  plus datasource identity.
- `skadi-sql-gateway` also has an in-process TTL cache for raw SQL results — this is a
  known duplication (debt item L5/L6 in dev-status).
- Lane C story C4 introduces `CacheContract` (TTL, strategy) and `CacheIdentity`
  (normalized SQL, principal, optional dataset version token) as Java records. These
  represent what the semantic layer will eventually pass *to* the cache layer — not
  ownership of the cache itself.
- ADR-005 (Proposed) includes a `cache.ttl` and `cache.strategy` field per contract.
  Those are *hints* from the contract, not a transfer of cache ownership.
- The two query paths today are independent (gateway → Databricks direct; future semantic
  layer → `skadi-server` → Databricks). A semantic-aware cache would make these paths
  share entries; a SQL-level cache already achieves this if the compiled SQL is identical.

---

## 2. Decision

**The Skadi cache remains positioned below all higher-level consumers and is not
semantic-aware in Lane C.**

### The two paths share the cache via normalised SQL

```text
Raw SQL path:
  SQL Gateway → SqlNormalizer → cache key → [hit] return
                                           → [miss] Databricks → cache write → return

Semantic path (future — activated post Lane C):
  Semantic Layer → SemanticCompiler → SqlNormalizer → cache key → [hit] return
                                                                 → [miss] skadi-server → Databricks → cache write → return
```

Both paths produce a normalised SQL string as the cache key. The cache does not know
whether a query originated from Tableau via pgwire or from an AI chat session via the
semantic API. The shared SQL key means that if two callers (one raw SQL, one semantic)
issue the same logical query, they share the same cached result.

### Cache ownership stays in `skadi-server`

`skadi-server` owns:

- Physical cache storage (in-process TTL map, optional S3 backend)
- Cache eviction policy and `maxEntries` config
- Cache metrics (`skadi_queries` `cache_tier` tags, Micrometer/Prometheus)

The semantic layer does **not** own the cache. It provides hints:

- `CacheContract.ttl()` — the TTL preference declared in the contract
- `CacheContract.strategy()` — `TTL` or `DATASET_VERSION` (dataset version deferred to
  post-Lane C; see gap L3)
- `CacheIdentity` — the normalised SQL + principal + optional version token that the
  semantic layer will compute before calling `skadi-server`

`skadi-server` may accept or override these hints according to its own configuration.
The contract declares a preference; the cache layer enforces policy.

### No cache rewrite in Lane C

Lane C story C4 introduces `CacheContract` and `CacheIdentity` as plain Java records.
It does **not**:

- Change cache storage in `skadi-server`
- Add semantic metadata to cache entries
- Add a second cache tier above the semantic layer
- Implement `DATASET_VERSION` strategy (deferred — see L3 and DQR-001 commentary)

### The distinction between logical identity and physical storage

`CacheIdentity` is a **logical** concept owned by the semantic layer — it describes
*what* is being cached. Physical storage (TTL map, S3) is owned by `skadi-server` —
it describes *where* and *how long*.

This distinction means the semantic layer can evolve its identity calculation
(e.g., adding a dataset version token) without touching cache storage, and the cache
can evolve its storage backend (e.g., switching to Redis) without touching the semantic
layer.

---

## 3. Rationale

**Why keep the cache below the semantic layer?**
A semantic-level cache (caching `SemanticQuery` → result) would need to:

- Know about `ContractRegistry` to validate cache keys
- Know about principal identity for access-controlled queries
- Be invalidated when contracts change (not just when data changes)

The SQL-level cache already handles principal-keyed results via normalised SQL + datasource.
Adding a second cache layer above the semantic layer introduces two invalidation surfaces
and increases the risk of serving stale results to unauthorised principals.

**Why not make the cache semantic-aware to improve hit rates?**
A semantic cache could cache `pnl_by_book` queries regardless of the specific SQL generated.
This is a valid optimisation for a future phase, but it requires the cache to understand
metric identity — coupling it to contract definitions. Before contract definitions are
stable, this coupling is premature. The hit-rate optimisation can be added post-Lane C
without changing the C4 interface design.

**Why `CacheIdentity` includes principal?**
Cache entries are per-user because row-level security in Databricks may return different
results for different principals on the same SQL. A cache keyed only on SQL could serve
one user's rows to another. Principal is always part of the logical identity.

**Why `strategy: DATASET_VERSION` is stubbed but not implemented?**
Dataset version (Delta snapshot ID or Skadi-managed version token) addresses the known
cache freshness gap L3. Including it in the `CacheIdentity` record from C4 means the
interface is ready when the version resolver is built. Implementing the resolver requires
a Delta API integration or a Skadi-managed version ledger — both out of scope for Lane C.

---

## 4. Consequences

### Positive

- Both query paths (raw SQL and future semantic) share cache entries when compiled SQL
  is identical — no duplicate storage overhead
- The cache layer has no dependency on semantic contract types — `skadi-server` can be
  upgraded independently
- `CacheIdentity` encapsulates the logical identity calculation in one place; all callers
  produce keys via the same type
- Dataset version caching (gap L3) has a defined extension point in `CacheIdentity`
  without requiring a cache rewrite

### Negative / Risks

- Different callers that produce logically equivalent queries but different SQL will get
  independent cache entries — a miss-rate cost that a semantic cache would avoid
- The `strategy: DATASET_VERSION` stub in C4 must not be called in production until the
  version resolver is implemented; a `UnsupportedOperationException` or explicit
  configuration guard is required
- `skadi-sql-gateway`'s separate in-process cache (debt L5/L6) duplicates entries that
  `skadi-server` already holds; deduplication is deferred beyond Lane C

### Operational Impact

- No changes to `skadi-server` cache storage in Lane C
- `CacheContract` and `CacheIdentity` records are compiled into `skadi-semantic` only;
  they are not on the `skadi-server` classpath
- Cache metrics remain unchanged; no new metrics required in Lane C

---

## 5. Alternatives Considered

| Option | Why Rejected |
| --- | --- |
| Semantic-level cache above `skadi-semantic` (cache `SemanticQuery` → result) | Requires cache to understand contract metadata; premature coupling before contracts are stable; introduces a second invalidation surface |
| Cache inside `skadi-semantic`, bypassing `skadi-server` | Duplicates storage; loses the shared-result benefit between raw SQL and semantic paths; adds S3 dependency to the semantic module |
| Shared cache tier separate from both modules | Adds infrastructure (e.g., Redis); premature for Lane C; deferred to a future observability/performance ADR |
| Remove `skadi-sql-gateway` in-process cache (consolidate now) | Debt L5/L6 — valid future work, but a Lane C distraction; leaves this for a dedicated cleanup story |

---

## 6. Fitness Functions / Enforcement

- `skadi-server` cache classes (`QueryCache`, `CacheProperties`) must not import any type
  from a `skadi-semantic` package. The dependency arrow goes one way: semantic layer knows
  about `CacheContract`; the cache does not know about semantic contracts.
- The `DATASET_VERSION` strategy in `CacheIdentity` must throw `UnsupportedOperationException`
  or be guarded by a feature flag until the version resolver is implemented.
- `CacheContract` and `CacheIdentity` records are Lane C C4 deliverables. They must not
  acquire methods that perform I/O, access Spring context, or call `skadi-server`.

> No automated ArchUnit enforcement planned for Lane C. Review-only.

---

## 7. Migration / Rollout Plan

1. **C4 (Lane C):** introduce `CacheContract` and `CacheIdentity` as plain records;
   `DATASET_VERSION` strategy is present but throws `UnsupportedOperationException`
2. **C5 (Lane C):** `SemanticExecutor` stub uses `CacheContract.ttl()` to annotate its
   fixture response; no real caching
3. **Post-DQR-002 (post Lane C):** when `SkadiserverSemanticExecutor` is connected, it
   passes `CacheIdentity` as a cache key hint to `skadi-server`
4. **Post-Lane C (gap L3):** implement `DATASET_VERSION` resolver; update `CacheIdentity`
   to populate the version token field

---

## 8. Open Questions

- Q1: Should `CacheContract` TTL be advisory (semantic layer suggests, cache decides) or
  mandatory (cache must respect the contract TTL)? Current decision: advisory.
- Q2: If the semantic compiler generates different SQL for the same `SemanticQuery` due to
  a contract change, should the old cache entry be evicted explicitly or expire naturally?
  Current decision: natural TTL expiry; explicit invalidation is a post-Lane C concern.
- Q3: When `skadi-sql-gateway` eventually routes through `skadi-server` (if DQR-002
  resolves in favour of convergence), the gateway's in-process cache becomes redundant.
  Who is responsible for removing it? Deferred to the convergence story.
