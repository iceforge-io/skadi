package org.iceforge.skadi.sqlgateway.cache;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class QueryResultCacheTest {

    private static final List<QueryResultCache.ColumnMeta> COLS =
            List.of(new QueryResultCache.ColumnMeta("id", 23));
    private static final List<String[]> ROWS = List.of(new String[]{"1"}, new String[]{"2"});

    @Test
    void returnsEmptyForUnknownKey() {
        QueryResultCache cache = new QueryResultCache(Clock.systemUTC(), 100);
        assertThat(cache.get("unknown")).isEmpty();
    }

    @Test
    void returnsStoredEntry() {
        QueryResultCache cache = new QueryResultCache(Clock.systemUTC(), 100);
        cache.put("key1", COLS, ROWS, Duration.ofMinutes(5));

        Optional<QueryResultCache.CacheEntry> result = cache.get("key1");
        assertThat(result).isPresent();
        assertThat(result.get().columns()).isEqualTo(COLS);
        assertThat(result.get().rows()).hasSize(2);
    }

    @Test
    void expiredEntryIsEvicted() {
        // Use a mutable clock so we can advance time without sleeping.
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2020-01-01T00:00:00Z"));
        Clock mutableClock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };

        QueryResultCache cache = new QueryResultCache(mutableClock, 100);
        cache.put("key", COLS, ROWS, Duration.ofSeconds(10)); // expiresAt = T + 10s

        // Before expiry: should be present.
        now.set(Instant.parse("2020-01-01T00:00:05Z")); // T + 5s
        assertThat(cache.get("key")).isPresent();

        // After expiry: should be evicted.
        now.set(Instant.parse("2020-01-01T00:00:11Z")); // T + 11s
        assertThat(cache.get("key")).isEmpty();
    }

    @Test
    void doesNotCacheWhenFull() {
        QueryResultCache cache = new QueryResultCache(Clock.systemUTC(), 2);
        cache.put("k1", COLS, ROWS, Duration.ofMinutes(5));
        cache.put("k2", COLS, ROWS, Duration.ofMinutes(5));
        cache.put("k3", COLS, ROWS, Duration.ofMinutes(5)); // over cap — should be skipped

        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.get("k3")).isEmpty();
    }

    @Test
    void nullKeyIsIgnored() {
        QueryResultCache cache = new QueryResultCache(Clock.systemUTC(), 100);
        cache.put(null, COLS, ROWS, Duration.ofMinutes(5)); // no exception
        assertThat(cache.get(null)).isEmpty();
    }
}
