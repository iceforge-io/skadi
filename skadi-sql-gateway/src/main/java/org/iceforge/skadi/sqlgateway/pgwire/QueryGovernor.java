package org.iceforge.skadi.sqlgateway.pgwire;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enforces per-user concurrency limits on active query execution.
 *
 * <p>Thread-safe: acquire/release are called from different session threads simultaneously.
 *
 * <p>When {@code maxPerUser <= 0} the governor is disabled and every {@link #tryAcquire} succeeds.
 */
final class QueryGovernor {

    private final int maxPerUser;
    private final ConcurrentHashMap<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    QueryGovernor(int maxPerUser) {
        this.maxPerUser = maxPerUser;
    }

    /**
     * Attempts to acquire a concurrency slot for {@code user}.
     *
     * @return {@code true} if the slot was acquired (query may proceed),
     *         {@code false} if the per-user limit is already reached
     */
    boolean tryAcquire(String user) {
        if (maxPerUser <= 0) return true; // unlimited
        String key = user == null ? "" : user;
        AtomicInteger counter = counts.computeIfAbsent(key, k -> new AtomicInteger(0));
        // CAS loop: check-then-increment atomically
        while (true) {
            int current = counter.get();
            if (current >= maxPerUser) return false;
            if (counter.compareAndSet(current, current + 1)) return true;
        }
    }

    /** Releases a concurrency slot previously acquired by {@link #tryAcquire}. */
    void release(String user) {
        if (maxPerUser <= 0) return;
        String key = user == null ? "" : user;
        AtomicInteger counter = counts.get(key);
        if (counter != null) {
            counter.decrementAndGet();
        }
    }

    /** Returns the number of currently active queries for a user. */
    int activeCount(String user) {
        if (maxPerUser <= 0) return 0;
        String key = user == null ? "" : user;
        AtomicInteger counter = counts.get(key);
        return counter == null ? 0 : Math.max(0, counter.get());
    }
}
