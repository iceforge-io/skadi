package org.iceforge.skadi.semantic.cache;

import java.util.Objects;

/**
 * The result of a cache lookup.
 *
 * <p>A {@code CacheLookupResult} carries the {@link CacheEntryState state} of
 * the lookup and, when an entry was found, the {@link CachedArtifactMeta metadata}
 * about that entry.
 *
 * <p>{@link #artifactMeta()} is {@code null} when {@link #state()} is
 * {@link CacheEntryState#ABSENT}. For all other states an entry was found and
 * {@code artifactMeta} is non-null.
 *
 * <p>Static factories cover the common cases:
 * <pre>{@code
 * CacheLookupResult miss    = CacheLookupResult.absent();
 * CacheLookupResult hit     = CacheLookupResult.hit(meta);
 * CacheLookupResult expired = CacheLookupResult.stale(meta);
 * }</pre>
 */
public record CacheLookupResult(
        CacheEntryState state,
        CachedArtifactMeta artifactMeta) {

    /**
     * @param state        the lifecycle state; must not be null
     * @param artifactMeta metadata about the found entry; must not be null
     *                     unless {@code state} is {@link CacheEntryState#ABSENT}
     */
    public CacheLookupResult {
        Objects.requireNonNull(state, "state must not be null");
        if (state != CacheEntryState.ABSENT && artifactMeta == null) {
            throw new IllegalArgumentException(
                    "artifactMeta must not be null when state is " + state);
        }
    }

    /** Returns a result representing a cache miss. */
    public static CacheLookupResult absent() {
        return new CacheLookupResult(CacheEntryState.ABSENT, null);
    }

    /**
     * Returns a result representing a valid cache hit.
     *
     * @param meta metadata about the found entry; must not be null
     */
    public static CacheLookupResult hit(CachedArtifactMeta meta) {
        Objects.requireNonNull(meta, "meta must not be null");
        return new CacheLookupResult(CacheEntryState.FRESH, meta);
    }

    /**
     * Returns a result representing a found-but-expired entry.
     *
     * @param meta metadata about the stale entry; must not be null
     */
    public static CacheLookupResult stale(CachedArtifactMeta meta) {
        Objects.requireNonNull(meta, "meta must not be null");
        return new CacheLookupResult(CacheEntryState.STALE, meta);
    }

    /**
     * Returns a result representing an explicitly invalidated entry.
     * Reserved for post-Lane C dataset-version invalidation (gap L3, DQR-001).
     *
     * @param meta metadata about the invalidated entry; must not be null
     */
    public static CacheLookupResult invalidated(CachedArtifactMeta meta) {
        Objects.requireNonNull(meta, "meta must not be null");
        return new CacheLookupResult(CacheEntryState.INVALIDATED, meta);
    }
}
