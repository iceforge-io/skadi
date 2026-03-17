package org.iceforge.skadi.sqlgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;
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
        Cache cache,
        Trace trace
) {

    public record PgWire(
            boolean enabled,
            String host,
            int port,
            Auth auth,
            Integer maxRows,
            Integer fetchSize,
            Duration queryTimeout,
            Integer maxConcurrentQueriesPerUser
    ) {
        /** Effective query timeout; null means no limit. */
        public Duration effectiveQueryTimeout() {
            return (queryTimeout == null || queryTimeout.isNegative() || queryTimeout.isZero())
                    ? null : queryTimeout;
        }

        /** 0 or negative means unlimited. */
        public int effectiveMaxConcurrentQueriesPerUser() {
            return (maxConcurrentQueriesPerUser == null || maxConcurrentQueriesPerUser <= 0)
                    ? 0 : maxConcurrentQueriesPerUser;
        }
        public record Auth(
                String mode,
                String credentialStore,                      // "plaintext" (default) | "bcrypt"
                Map<String, String> users,
                Map<String, PolicyConfig> policies           // username → schema ACL
        ) {
            public record PolicyConfig(List<String> allowedSchemas) {}
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
     * Tableau trace mode configuration.
     */
    public record Trace(
            boolean enabled,
            String testdataPath
    ) {
        public boolean isEnabled() {
            return enabled;
        }
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
