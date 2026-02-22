package org.iceforge.skadi.api;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.LongAdder;

/**
 * Minimal in-memory cache metrics for the dashboard.
 * <p>
 * Tracks whether submissions were cache hits (manifest/object existed)
 * or misses (required materialization / waiting for another writer).
 */
@Component
public class CacheMetricsRegistry {
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public void recordHit() { hits.increment(); }
    public void recordMiss() { misses.increment(); }

    public long hits() { return hits.sum(); }
    public long misses() { return misses.sum(); }
}
