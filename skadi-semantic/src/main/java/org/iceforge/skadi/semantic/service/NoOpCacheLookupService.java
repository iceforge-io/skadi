package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.cache.CacheLookupRequest;
import org.iceforge.skadi.semantic.cache.CacheLookupResult;
import org.iceforge.skadi.semantic.cache.CacheWriteRequest;

import java.util.Objects;

/**
 * A no-op {@link CacheLookupService} for tests and inactive deployments.
 *
 * <p>Every lookup returns {@link CacheLookupResult#absent()} — as if the cache
 * is always empty. Every write is silently discarded. No storage is read or
 * written.
 *
 * <p>Use this implementation in unit tests that need a {@link CacheLookupService}
 * instance without a real cache backend, or in configurations where caching is
 * explicitly disabled.
 */
public final class NoOpCacheLookupService implements CacheLookupService {

    /** Singleton instance — this class has no state. */
    public static final NoOpCacheLookupService INSTANCE = new NoOpCacheLookupService();

    @Override
    public CacheLookupResult lookup(CacheLookupRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return CacheLookupResult.absent();
    }

    @Override
    public void write(CacheWriteRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        // intentionally no-op
    }
}
