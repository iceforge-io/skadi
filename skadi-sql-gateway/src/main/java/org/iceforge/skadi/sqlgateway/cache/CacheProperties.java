package org.iceforge.skadi.sqlgateway.cache;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "skadi.sql-gateway.cache")
public record CacheProperties(
        boolean enabled,
        Duration metadataTtl,
        Duration queryResultTtl
) {
    public CacheProperties {
        if (metadataTtl == null) metadataTtl = Duration.ofMinutes(2);
        if (queryResultTtl == null) queryResultTtl = Duration.ofMinutes(2);
    }
}

