package org.iceforge.skadi.demo.h2;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Demo / integration-test-only H2 configuration.
 *
 * Opt-in via: skadi.demo.h2.enabled=true
 */
@ConfigurationProperties(prefix = "skadi.demo.h2")
public class DemoH2Properties {

    /** Enable the embedded in-memory H2 dataset + admin endpoints. */
    private boolean enabled = false;

    /** H2 in-memory database name (jdbc:h2:mem:{dbName}). */
    private String dbName = "skadi_demo";

    /** Optional override for the JDBC URL. If set, takes precedence over dbName. */
    private String jdbcUrl;

    /** H2 username (default H2 is "sa"). */
    private String username = "sa";

    /** H2 password (default empty for H2). */
    private String password = "";

    /**
     * Require callers to present this token in header X-Skadi-Demo-Token.
     * If blank, token auth is disabled.
     */
    private String adminToken;

    /**
     * If false (default), admin endpoints only accept requests from localhost.
     */
    private boolean allowRemote = false;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }

    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getAdminToken() { return adminToken; }
    public void setAdminToken(String adminToken) { this.adminToken = adminToken; }

    public boolean isAllowRemote() { return allowRemote; }
    public void setAllowRemote(boolean allowRemote) { this.allowRemote = allowRemote; }
}
