package org.iceforge.skadi.semantic.cache;

import java.util.Objects;

/**
 * A request to look up a cached result for a given query identity.
 *
 * <p>A {@code CacheLookupRequest} bundles the logical query identity with the
 * cache policy hint that was declared for this query. The cache layer uses
 * the {@link CacheContract} to decide whether a found entry is still valid
 * (e.g., within the declared TTL).
 *
 * <p>This record does not perform any cache lookup — it is the input to one.
 *
 * <pre>{@code
 * var req = new CacheLookupRequest(
 *     new CacheIdentity("SELECT ...", "alice", null),
 *     CacheContract.ttl(300L));
 * }</pre>
 */
public record CacheLookupRequest(
        CacheIdentity identity,
        CacheContract contract) {

    /**
     * @param identity the logical query identity; must not be null
     * @param contract the cache policy hint; must not be null
     */
    public CacheLookupRequest {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(contract, "contract must not be null");
    }
}
