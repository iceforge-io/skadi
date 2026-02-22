package org.iceforge.skadi.jdbc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Server-side JDBC datasource configuration.
 * <p>
 * Corporate clients should configure datasources here (or via Vault/Secrets Manager indirection)
 * and pass only a {@code datasourceId} in API requests.
 */
@ConfigurationProperties(prefix = "skadi.jdbc")
public class SkadiJdbcProperties {

    /** Map of datasourceId -> config. */
    private Map<String, JdbcDatasourceConfig> datasources = new HashMap<>();

    public Map<String, JdbcDatasourceConfig> getDatasources() {
        return datasources;
    }

    public void setDatasources(Map<String, JdbcDatasourceConfig> datasources) {
        this.datasources = datasources;
    }

    /**
     * One datasource configuration.
     *
     * NOTE: storing secrets directly in YAML is fine for local dev, but corp deployments should
     * wire these via environment variables, a secrets manager, or a provider plugin.
     */
    public static class JdbcDatasourceConfig {

        /** Optional forced provider id (e.g. "default", "corp-iam", "corp-bridge"). */
        private String provider;

        /** JDBC URL. */
        private String jdbcUrl;

        /** Optional username (legacy/basic auth scenarios). */
        private String username;

        /** Optional password (legacy/basic auth scenarios). */
        private String password;

        /** Provider-specific properties (non-secret preferred). */
        private Map<String, String> properties = new HashMap<>();

        /** Optional tags used by providers for routing/behavior (non-secret). */
        private Map<String, String> tags = new HashMap<>();

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }
    }
}
