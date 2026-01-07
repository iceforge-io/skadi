package org.iceforge.skadi.jdbc.spi;

import org.iceforge.skadi.jdbc.SkadiJdbcProperties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

/**
 * Default JDBC provider that uses DriverManager.
 * <p>
 * This is intentionally simple and works in local dev.
 * Production deployments likely prefer a provider that returns pooled DataSources
 * and/or uses enterprise authentication flows.
 */
public class DefaultDriverManagerJdbcConnectionProvider implements JdbcConnectionProvider {

    @Override
    public String id() {
        return "default";
    }

    @Override
    public boolean supports(JdbcClientContext context) {
        // Always supports; used as a fallback if no other providers match.
        return true;
    }

    @Override
    public Connection openConnection(JdbcClientContext context, SkadiJdbcProperties.JdbcDatasourceConfig cfg) throws Exception {
        String url = cfg.getJdbcUrl();
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl is required for datasourceId=" + context.datasourceId().orElse("<legacy>"));
        }

        Properties props = new Properties();
        Map<String, String> m = cfg.getProperties();
        if (m != null) {
            for (Map.Entry<String, String> e : m.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    props.put(e.getKey(), e.getValue());
                }
            }
        }

        // Username/password are OPTIONAL; some JDBC drivers use properties instead.
        if (cfg.getUsername() != null && !cfg.getUsername().isBlank()) {
            props.put("user", cfg.getUsername());
            if (cfg.getPassword() != null) {
                props.put("password", cfg.getPassword());
            }
        }

        if (!props.isEmpty()) {
            return DriverManager.getConnection(url, props);
        }

        return DriverManager.getConnection(url);
    }
}
