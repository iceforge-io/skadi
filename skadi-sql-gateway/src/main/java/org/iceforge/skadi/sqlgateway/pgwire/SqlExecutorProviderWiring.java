package org.iceforge.skadi.sqlgateway.pgwire;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Wires a {@link SqlExecutorProvider} into the pgwire layer.
 *
 * <p>PgWireSession is constructed manually, so we bridge via {@link SqlExecutorProviderHolder}.
 */
@Configuration
@ConditionalOnBean(name = "databricksDataSource")
class SqlExecutorProviderWiring {

    SqlExecutorProviderWiring(DataSource dataSource) {
        SqlExecutorProviderHolder.set(dataSource::getConnection);
    }
}
