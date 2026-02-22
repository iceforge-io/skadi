package org.iceforge.skadi.sqlgateway.pgwire;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Bridge until PgWireSession is refactored into a Spring-managed component.
 *
 * <p>MVP: PgWireSession is created manually by PgWireServer, so we can't inject dependencies
 * directly; we store an optional provider here.
 */
final class SqlExecutorProviderHolder {
    private static final AtomicReference<SqlExecutorProvider> REF = new AtomicReference<>();

    private SqlExecutorProviderHolder() {}

    static SqlExecutorProvider get() {
        return REF.get();
    }

    static void set(SqlExecutorProvider provider) {
        REF.set(provider);
    }
}

