package org.iceforge.skadi.semantic.cache;

import java.util.Objects;

/**
 * A request to write a query result to the cache.
 *
 * <p>A {@code CacheWriteRequest} bundles the logical identity, the policy hint
 * that governs this entry's lifetime, and the estimated size of the result to
 * be stored. The cache layer uses these inputs to decide whether to accept the
 * write (e.g., reject if the entry would exceed a size limit) and how long to
 * retain it.
 *
 * <p>This record does not perform any cache write — it is the input to one.
 * {@link #estimatedSizeBytes()} may be 0 if the size is not known before
 * the write completes.
 *
 * <pre>{@code
 * var req = new CacheWriteRequest(
 *     new CacheIdentity("SELECT ...", "alice", null),
 *     CacheContract.ttl(300L),
 *     8192L);
 * }</pre>
 */
public record CacheWriteRequest(
        CacheIdentity identity,
        CacheContract contract,
        long estimatedSizeBytes) {

    /**
     * @param identity             the logical query identity; must not be null
     * @param contract             the cache policy governing this entry; must not be null
     * @param estimatedSizeBytes   estimated size in bytes; 0 if unknown; must be >= 0
     */
    public CacheWriteRequest {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(contract, "contract must not be null");
        if (estimatedSizeBytes < 0) {
            throw new IllegalArgumentException(
                    "estimatedSizeBytes must be >= 0, was: " + estimatedSizeBytes);
        }
    }
}
