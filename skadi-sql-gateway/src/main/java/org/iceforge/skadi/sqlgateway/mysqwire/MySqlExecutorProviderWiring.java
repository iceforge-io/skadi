package org.iceforge.skadi.sqlgateway.mysqwire;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Wires the shared Databricks {@link DataSource} into the MySQL wire layer.
 * Mirrors the pgwire {@code SqlExecutorProviderWiring} pattern.
 */
@Configuration
@ConditionalOnBean(name = "databricksDataSource")
class MySqlExecutorProviderWiring {

    MySqlExecutorProviderWiring(DataSource dataSource) {
        MySqlExecutorProviderHolder.set(dataSource::getConnection);
    }
}
