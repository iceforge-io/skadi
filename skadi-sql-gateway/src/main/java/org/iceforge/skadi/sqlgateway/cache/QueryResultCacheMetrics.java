package org.iceforge.skadi.sqlgateway.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tiny metrics holder for cache hit/miss.
 *
 * <p>MVP: exposed via /sql-gateway and logs; can be bridged to Micrometer later.
 */
public final class QueryResultCacheMetrics {
    private final AtomicLong hit = new AtomicLong();
    private final AtomicLong miss = new AtomicLong();

    public void recordHit() {
        hit.incrementAndGet();
    }

    public void recordMiss() {
        miss.incrementAndGet();
    }

    public long hits() {
        return hit.get();
    }

    public long misses() {
        return miss.get();
    }
}

