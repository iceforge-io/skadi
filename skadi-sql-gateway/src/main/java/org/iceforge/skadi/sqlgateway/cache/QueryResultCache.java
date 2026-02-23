package org.iceforge.skadi.sqlgateway.cache;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory TTL cache for Arrow IPC bytes.
 *
 * <p>POC scope: single-node local cache. Future: optional S3-backed cache.
 */
public final class QueryResultCache {

    public record Entry(byte[] arrowBytes, Instant expiresAt, long sizeBytes) {
    }

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();
    private final Clock clock;

    public QueryResultCache(Clock clock) {
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public Optional<Entry> get(String key) {
        if (key == null) return Optional.empty();
        Entry e = map.get(key);
        if (e == null) return Optional.empty();
        if (clock.instant().isAfter(e.expiresAt())) {
            map.remove(key, e);
            return Optional.empty();
        }
        return Optional.of(e);
    }

    public void put(String key, byte[] arrowBytes, Duration ttl) {
        if (key == null || arrowBytes == null) return;
        Duration t = (ttl == null || ttl.isNegative() || ttl.isZero()) ? Duration.ofMinutes(2) : ttl;
        map.put(key, new Entry(arrowBytes, clock.instant().plus(t), arrowBytes.length));
    }

    public static ByteArrayOutputStream newBuffer() {
        // Avoid tiny default.
        return new ByteArrayOutputStream(64 * 1024);
    }
}

