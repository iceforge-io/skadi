package org.iceforge.skadi.sqlgateway.pgwire;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Temporary bridge to allow {@link PgWireSession} (manually constructed) to use
 * Spring-wired helpers like result caching.
 */
public final class PgWireSessionCachingBridge {
    private static final AtomicReference<PgWireQueryCachingExecutorProvider> REF = new AtomicReference<>();

    private PgWireSessionCachingBridge() {
    }

    static PgWireQueryCachingExecutorProvider get() {
        return REF.get();
    }

    public static void set(PgWireQueryCachingExecutorProvider provider) {
        REF.set(provider);
    }
}
