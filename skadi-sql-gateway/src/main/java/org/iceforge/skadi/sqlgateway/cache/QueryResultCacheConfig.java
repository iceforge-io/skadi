package org.iceforge.skadi.sqlgateway.cache;

import org.iceforge.skadi.sqlgateway.metadata.MetadataCache;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties(CacheProperties.class)
public class QueryResultCacheConfig {

    @Bean
    public MetadataCache metadataCache() {
        // Keep a single app-wide cache instance; router already does TTL per-entry.
        return new MetadataCache(Clock.systemUTC());
    }

    @Bean
    @ConditionalOnProperty(prefix = "skadi.sql-gateway.cache", name = "enabled", havingValue = "true")
    public QueryResultCache queryResultCache() {
        return new QueryResultCache(Clock.systemUTC(), 500);
    }

    @Bean
    @ConditionalOnProperty(prefix = "skadi.sql-gateway.cache", name = "enabled", havingValue = "true")
    public QueryResultCacheMetrics queryResultCacheMetrics() {
        return new QueryResultCacheMetrics();
    }
}

