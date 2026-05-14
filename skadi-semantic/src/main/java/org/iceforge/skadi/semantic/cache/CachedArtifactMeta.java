package org.iceforge.skadi.semantic.cache;

import java.time.Instant;
import java.util.Objects;

/**
 * Metadata describing a stored cache entry.
 *
 * <p>A {@code CachedArtifactMeta} records what was stored, where it was stored,
 * how large it is, and when it expires. It is a descriptor — it does not open
 * storage connections or read the actual cached bytes.
 *
 * <p>{@link #expiresAt()} is {@code null} when no expiry is declared (e.g.
 * for {@link CacheStorageBackend#S3}-backed entries under a dataset-version
 * strategy where expiry is by version change, not by time).
 *
 * <pre>{@code
 * var meta = new CachedArtifactMeta(
 *     identity,
 *     CacheStorageBackend.LOCAL,
 *     4096L,
 *     Instant.now(),
 *     Instant.now().plusSeconds(300));
 * }</pre>
 */
public record CachedArtifactMeta(
        CacheIdentity identity,
        CacheStorageBackend storageBackend,
        long sizeBytes,
        Instant cachedAt,
        Instant expiresAt) {

    /**
     * @param identity       the logical identity of the cached query; must not be null
     * @param storageBackend the backend where the artifact is stored; must not be null
     * @param sizeBytes      size of the cached artifact in bytes; 0 if unknown
     * @param cachedAt       when the entry was written to the cache; must not be null
     * @param expiresAt      when the entry expires; null if no time-based expiry applies
     */
    public CachedArtifactMeta {
        Objects.requireNonNull(identity,       "identity must not be null");
        Objects.requireNonNull(storageBackend, "storageBackend must not be null");
        Objects.requireNonNull(cachedAt,       "cachedAt must not be null");
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be >= 0, was: " + sizeBytes);
        }
    }
}
