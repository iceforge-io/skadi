package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.cache.CacheProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Wires the in-memory pgwire row-set cache when caching is enabled.
 */
@Configuration
@ConditionalOnProperty(prefix = "skadi.sql-gateway.cache", name = "enabled", havingValue = "true")
class PgWireRowSetCacheWiring {

    PgWireRowSetCacheWiring(CacheProperties props) {
        org.iceforge.skadi.sqlgateway.pgwire.PgWireSessionRowSetCacheBridge.set(
                new PgWireRowSetCache(Clock.systemUTC()),
                props.queryResultTtl()
        );
    }
}
