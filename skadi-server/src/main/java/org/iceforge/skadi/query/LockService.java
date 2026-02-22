package org.iceforge.skadi.query;

/**
 * Simple distributed (or local) lock abstraction used by QueryService to ensure
 * only one writer materializes a given query cache key.
 */
public interface LockService {

    boolean tryAcquire(String bucket, String key, String owner, long ttlSeconds);

    void release(String bucket, String key);
}
