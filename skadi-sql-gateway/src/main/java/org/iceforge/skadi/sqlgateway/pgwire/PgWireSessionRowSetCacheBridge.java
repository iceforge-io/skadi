package org.iceforge.skadi.sqlgateway.pgwire;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/** Temporary bridge to allow {@link PgWireSession} to use Spring-created cache instances. */
public final class PgWireSessionRowSetCacheBridge {
    private static final AtomicReference<PgWireRowSetCache> CACHE = new AtomicReference<>();
    private static final AtomicReference<Duration> TTL = new AtomicReference<>();

    private PgWireSessionRowSetCacheBridge() {
    }

    public static void set(PgWireRowSetCache cache, Duration ttl) {
        CACHE.set(cache);
        TTL.set(ttl);
    }

    static PgWireRowSetCache getCache() {
        return CACHE.get();
    }

    static Duration getTtl() {
        return TTL.get();
    }
}
