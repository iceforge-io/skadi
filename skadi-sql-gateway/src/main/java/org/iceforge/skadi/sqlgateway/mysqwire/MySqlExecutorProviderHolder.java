package org.iceforge.skadi.sqlgateway.mysqwire;

import org.iceforge.skadi.sqlgateway.pgwire.SqlExecutorProvider;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Static bridge giving manually-constructed {@link MySqlWireSession} access to the
 * shared JDBC executor — mirrors the pattern used in the pgwire layer.
 */
final class MySqlExecutorProviderHolder {
    private static final AtomicReference<SqlExecutorProvider> REF = new AtomicReference<>();

    private MySqlExecutorProviderHolder() {}

    static SqlExecutorProvider get() { return REF.get(); }
    static void set(SqlExecutorProvider p) { REF.set(p); }
}
