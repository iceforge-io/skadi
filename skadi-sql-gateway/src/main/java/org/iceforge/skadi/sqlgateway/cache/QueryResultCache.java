package org.iceforge.skadi.sqlgateway.cache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-process TTL cache for real query result rows. */
public final class QueryResultCache {

    public record ColumnMeta(String name, int pgOid) {}

    private record Entry(List<ColumnMeta> columns, List<String[]> rows, Instant expiresAt) {}

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();
    private final Clock clock;
    private final int maxEntries;

    public QueryResultCache(Clock clock, int maxEntries) {
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.maxEntries = maxEntries <= 0 ? 500 : maxEntries;
    }

    public record CacheEntry(List<ColumnMeta> columns, List<String[]> rows) {}

    public Optional<CacheEntry> get(String key) {
        if (key == null) return Optional.empty();
        Entry e = map.get(key);
        if (e == null) return Optional.empty();
        if (clock.instant().isAfter(e.expiresAt())) {
            map.remove(key, e);
            return Optional.empty();
        }
        return Optional.of(new CacheEntry(e.columns(), e.rows()));
    }

    public void put(String key, List<ColumnMeta> columns, List<String[]> rows, Duration ttl) {
        if (key == null || columns == null || rows == null) return;
        if (map.size() >= maxEntries) return; // cap: don't evict, just skip
        Duration t = (ttl == null || ttl.isNegative() || ttl.isZero()) ? Duration.ofMinutes(5) : ttl;
        map.put(key, new Entry(columns, rows, clock.instant().plus(t)));
    }

    public int size() {
        return map.size();
    }
}
