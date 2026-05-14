package org.iceforge.skadi.semantic.cache;

/**
 * Lifecycle state of a cache entry as observed by a lookup.
 *
 * <p>This enum is a descriptor — it communicates the state result of a
 * cache lookup without carrying any storage or eviction logic.
 */
public enum CacheEntryState {

    /**
     * An entry was found and is within its TTL or version validity window.
     * The caller may serve the cached result directly.
     */
    FRESH,

    /**
     * An entry was found but has exceeded its TTL.
     * The caller should re-execute the query and write a new entry.
     * A stale entry may still be returned as a best-effort response
     * if the caller chooses to tolerate staleness.
     */
    STALE,

    /**
     * No entry was found for the given {@link CacheIdentity}.
     * The caller must execute the query and optionally write the result.
     */
    ABSENT,

    /**
     * An entry was explicitly invalidated (e.g. by a dataset version change).
     * Reserved as a future extension seam for dataset-version invalidation
     * (gap L3, DQR-001). Not populated in Lane C.
     */
    INVALIDATED
}
