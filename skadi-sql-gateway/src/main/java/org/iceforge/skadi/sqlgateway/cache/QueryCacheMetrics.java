package org.iceforge.skadi.sqlgateway.cache;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe hit/miss counters for the local query result cache. */
public final class QueryCacheMetrics {

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public void recordHit() {
        hits.increment();
    }

    public void recordMiss() {
        misses.increment();
    }

    public long hits() {
        return hits.sum();
    }

    public long misses() {
        return misses.sum();
    }
}
