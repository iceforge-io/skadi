/**
 * Cache boundary contracts — Lane C C4.
 *
 * <p>Types in this package define the logical boundary between query identity,
 * execution, cached result metadata, and physical cache storage. They are the
 * interface surface that the semantic layer (and future callers) use to express
 * caching intent without depending on cache implementation details.
 *
 * <h2>Types</h2>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheStorageBackend} —
 *       enum: LOCAL / S3 / OBJECT_STORE; physical backend abstraction</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheEntryState} —
 *       enum: FRESH / STALE / ABSENT / INVALIDATED; lifecycle state descriptor</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheIdentity} —
 *       logical query identity: normalised SQL + principal + optional dataset
 *       version token; {@code fingerprint()} computes a deterministic SHA-256
 *       hex string usable as a physical cache key</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheContract} —
 *       policy hint passed from the semantic layer to the cache layer: strategy
 *       + TTL; {@code from(SemanticCachePolicy)} converts from the C2 descriptor</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CachedArtifactMeta} —
 *       metadata about a stored cache entry: identity, backend, size, timestamps</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheLookupRequest} —
 *       a lookup request carrying identity and policy hint</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheLookupResult} —
 *       lookup result: state + optional artifact metadata</li>
 *   <li>{@link org.iceforge.skadi.semantic.cache.CacheWriteRequest} —
 *       a write request carrying identity, policy hint, and size estimate</li>
 * </ul>
 *
 * <h2>Design invariants</h2>
 * <p>No type here:
 * <ul>
 *   <li>performs cache reads or writes</li>
 *   <li>connects to S3, local disk, or any storage backend</li>
 *   <li>changes existing cache behaviour in {@code skadi-server} or
 *       {@code skadi-sql-gateway}</li>
 *   <li>contains Spring beans or runtime wiring</li>
 *   <li>is aware of semantic contracts, measures, or dimensions</li>
 * </ul>
 *
 * <p>See {@code ai/adr/ADR-010-cache-positioning.md} for the positioning
 * decision, and {@code ai/dqr/DQR-001-contract-definition-format.md} for
 * the open question on dataset version token resolution.
 */
package org.iceforge.skadi.semantic.cache;
