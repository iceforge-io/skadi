package org.iceforge.skadi.sqlgateway.metadata;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Simple TTL cache for metadata rowsets. */
public final class MetadataCache {

    private record Entry(MetadataRowSet value, Instant expiresAt) {}

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();
    private final Clock clock;

    public MetadataCache(Clock clock) {
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public Optional<MetadataRowSet> get(String key) {
        if (key == null) return Optional.empty();
        Entry e = map.get(key);
        if (e == null) return Optional.empty();
        if (clock.instant().isAfter(e.expiresAt())) {
            map.remove(key, e);
            return Optional.empty();
        }
        return Optional.of(e.value());
    }

    public void put(String key, MetadataRowSet value, Duration ttl) {
        if (key == null || value == null) return;
        Duration t = (ttl == null || ttl.isNegative() || ttl.isZero()) ? Duration.ofMinutes(2) : ttl;
        map.put(key, new Entry(value, clock.instant().plus(t)));
    }
}

