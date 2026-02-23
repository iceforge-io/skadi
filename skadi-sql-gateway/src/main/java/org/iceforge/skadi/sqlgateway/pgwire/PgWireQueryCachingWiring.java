package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.cache.CacheProperties;
import org.iceforge.skadi.sqlgateway.cache.QueryResultCache;
import org.iceforge.skadi.sqlgateway.cache.QueryResultCacheMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Installs the POC query-result cache into pgwire execution.
 */
@Configuration
@ConditionalOnBean(name = "databricksDataSource")
@ConditionalOnProperty(prefix = "skadi.sql-gateway.cache", name = "enabled", havingValue = "true")
class PgWireQueryCachingWiring {

    PgWireQueryCachingWiring(DataSource dataSource,
                             QueryResultCache cache,
                             QueryResultCacheMetrics metrics,
                             CacheProperties props) {
        PgWireSessionCachingBridge.set(new PgWireQueryCachingExecutorProvider(
                dataSource,
                cache,
                metrics,
                props.queryResultTtl()
        ));
    }
}

