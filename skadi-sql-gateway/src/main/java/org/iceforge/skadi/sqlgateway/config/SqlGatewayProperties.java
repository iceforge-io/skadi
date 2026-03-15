package org.iceforge.skadi.sqlgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
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
        PgWire pgwire,
        Metadata metadata,
        Cache cache
) {

    public record PgWire(
            boolean enabled,
            String host,
            int port,
            Auth auth,
            Integer maxRows,
            Integer fetchSize
    ) {
        public record Auth(
                String mode,
                Map<String, String> users
        ) {
        }
    }

    /**
     * Metadata discovery facade configuration.
     *
     * <p>MVP mapping:
     * <ul>
     *   <li>DBX catalog -> PG database name</li>
     *   <li>DBX schema  -> PG schema name</li>
     * </ul>
     */
    public record Metadata(
            boolean enabled,
            Duration ttl,
            String pgDatabase,
            String dbxCatalog,
            String dbxSchema
    ) {
    }

    /**
     * Query result cache configuration (POC: local in-memory only).
     */
    public record Cache(
            boolean enabled,
            Duration ttl,
            Integer maxEntries
    ) {
        public boolean isEnabled() {
            return enabled;
        }

        public Duration effectiveTtl() {
            return (ttl == null || ttl.isNegative() || ttl.isZero()) ? Duration.ofMinutes(5) : ttl;
        }

        public int effectiveMaxEntries() {
            return (maxEntries == null || maxEntries <= 0) ? 500 : maxEntries;
        }
    }
}
