package org.iceforge.skadi.sqlgateway.pgwire;

import java.sql.Connection;

/**
 * Very small SPI for pgwire to obtain a JDBC {@link Connection}.
 *
 * <p>Wired by Spring in the gateway app when a backend executor is enabled.
 */
public interface SqlExecutorProvider {
    Connection getConnection() throws Exception;
}

