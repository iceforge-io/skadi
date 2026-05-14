package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.cache.CacheLookupRequest;
import org.iceforge.skadi.semantic.cache.CacheLookupResult;
import org.iceforge.skadi.semantic.cache.CacheWriteRequest;

/**
 * Service seam for logical cache lookup and write.
 *
 * <p>This interface depends on the C4 boundary types ({@link CacheLookupRequest},
 * {@link CacheLookupResult}, {@link CacheWriteRequest}) rather than on physical
 * cache internals. Implementations may route to the in-process
 * {@code QueryResultCache} in the gateway, to the S3-backed cache in
 * {@code skadi-server}, or to any future storage backend.
 *
 * <p>The interface does not expose eviction, invalidation, or storage-backend
 * configuration — those remain in the physical cache implementations.
 * The semantic layer calls this interface without knowing whether the storage
 * is local, S3, or another backend.
 *
 * <p>A no-op implementation is available for tests and inactive deployments:
 * {@link NoOpCacheLookupService}.
 *
 * <p><strong>This interface is not a Spring bean in Lane C.</strong>
 */
public interface CacheLookupService {

    /**
     * Performs a logical cache lookup.
     *
     * @param request the lookup request carrying identity and policy hint; must not be null
     * @return the lookup result; never null
     */
    CacheLookupResult lookup(CacheLookupRequest request);

    /**
     * Records a cache write intent.
     *
     * <p>Implementations may write immediately, queue the write, or silently
     * discard it if caching is disabled or the entry exceeds a size limit.
     * Callers must not rely on the write being durably committed before this
     * method returns.
     *
     * @param request the write request carrying identity, policy hint, and size; must not be null
     */
    void write(CacheWriteRequest request);
}
