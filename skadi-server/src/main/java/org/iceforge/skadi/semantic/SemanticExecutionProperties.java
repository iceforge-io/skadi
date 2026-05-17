package org.iceforge.skadi.semantic;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Lane E semantic execution delegation.
 *
 * <pre>{@code
 * skadi:
 *   semantic:
 *     execution:
 *       server-url: http://localhost:8080   # skadi-server query API base URL
 *       datasource-id: default             # which skadi.jdbc.datasources entry to use
 * }</pre>
 *
 * <p>{@code server-url} defaults to {@code http://localhost:8080} so single-instance
 * deployments work without additional configuration. In production, set this to the
 * reachable base URL of the skadi-server instance that owns the Databricks JDBC pool.
 *
 * <p>{@code datasource-id} must match a key in {@code skadi.jdbc.datasources}.
 * The resolved datasource's JDBC URL and credentials are forwarded with each query request.
 */
@ConfigurationProperties(prefix = "skadi.semantic.execution")
public class SemanticExecutionProperties {

    private String serverUrl = "http://localhost:8080";
    private String datasourceId = "default";

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getDatasourceId() {
        return datasourceId;
    }

    public void setDatasourceId(String datasourceId) {
        this.datasourceId = datasourceId;
    }
}
