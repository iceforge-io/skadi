package org.iceforge.skadi.sqlgateway.pgwire;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * POC row-set cache for pgwire responses.
 *
 * <p>Stores already materialized rows (as text values) along with column labels.
 * This is safe for a prototype but not suitable for very large results.
 */
public final class PgWireRowSetCache {

    public record RowSet(String[] columns, List<String[]> rows, String commandTag, Instant expiresAt) {
    }

    private final ConcurrentHashMap<String, RowSet> map = new ConcurrentHashMap<>();
    private final Clock clock;

    public PgWireRowSetCache(Clock clock) {
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public Optional<RowSet> get(String key) {
        if (key == null) return Optional.empty();
        RowSet rs = map.get(key);
        if (rs == null) return Optional.empty();
        if (clock.instant().isAfter(rs.expiresAt())) {
            map.remove(key, rs);
            return Optional.empty();
        }
        return Optional.of(rs);
    }

    public void put(String key, String[] columns, List<String[]> rows, String commandTag, Duration ttl) {
        if (key == null || columns == null || rows == null) return;
        Duration t = (ttl == null || ttl.isNegative() || ttl.isZero()) ? Duration.ofMinutes(2) : ttl;
        map.put(key, new RowSet(columns, rows, commandTag, clock.instant().plus(t)));
    }
}

