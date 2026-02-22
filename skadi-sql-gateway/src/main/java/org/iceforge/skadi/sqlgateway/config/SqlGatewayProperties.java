package org.iceforge.skadi.sqlgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * Configuration stub for the SQL Gateway.
 *
 * <p>This will grow as we add the wire protocol listener and JDBC/DBSQL execution.
 */
@ConfigurationProperties(prefix = "skadi.sql-gateway")
public record SqlGatewayProperties(
        String advertisedHost,
        Integer advertisedPort,
        PgWire pgwire
) {

    public record PgWire(
            boolean enabled,
            String host,
            int port,
            Auth auth
    ) {
        public record Auth(
                String mode,
                Map<String, String> users
        ) {
        }
    }
}
