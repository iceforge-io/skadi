package org.iceforge.skadi.sqlgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

@ConfigurationProperties(prefix = "skadi.sql-gateway.databricks")
public record DatabricksProperties(
        boolean enabled,
        String host,
        String httpPath,
        String token,
        int maxPoolSize,
        Duration connectTimeout,
        Duration queryTimeout,
        Map<String, String> jdbcProperties
) {
}

