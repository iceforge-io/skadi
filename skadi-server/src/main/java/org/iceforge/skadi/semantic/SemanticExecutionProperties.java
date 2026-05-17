package org.iceforge.skadi.semantic;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Lane E/F semantic execution delegation.
 *
 * <pre>{@code
 * skadi:
 *   semantic:
 *     execution:
 *       enabled: true                        # false = DISABLED; no calls attempted
 *       server-url: http://localhost:8080    # skadi-server query API base URL
 *       datasource-id: default              # which skadi.jdbc.datasources entry to use
 *       circuit-breaker:
 *         enabled: true                      # enable circuit-breaker protection
 *         failure-threshold: 3               # consecutive failures before opening circuit
 *         open-duration-ms: 30000            # ms the circuit stays open before probe
 * }</pre>
 */
@ConfigurationProperties(prefix = "skadi.semantic.execution")
public class SemanticExecutionProperties {

    /** Whether semantic execution delegation is active. {@code false} → DISABLED, no HTTP calls. */
    private boolean enabled = true;
    private String serverUrl = "http://localhost:8080";
    private String datasourceId = "default";
    private CircuitBreakerProperties circuitBreaker = new CircuitBreakerProperties();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getServerUrl() { return serverUrl; }
    public void setServerUrl(String serverUrl) { this.serverUrl = serverUrl; }

    public String getDatasourceId() { return datasourceId; }
    public void setDatasourceId(String datasourceId) { this.datasourceId = datasourceId; }

    public CircuitBreakerProperties getCircuitBreaker() { return circuitBreaker; }
    public void setCircuitBreaker(CircuitBreakerProperties circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    /** Circuit-breaker sub-properties. */
    public static class CircuitBreakerProperties {
        /** Whether the circuit-breaker is active. When false, failures are recorded but the circuit never opens. */
        private boolean enabled = true;
        /** Consecutive failures before opening the circuit. */
        private int failureThreshold = 3;
        /** Duration in milliseconds the circuit stays open before allowing a probe call. */
        private long openDurationMs = 30_000L;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public int getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }

        public long getOpenDurationMs() { return openDurationMs; }
        public void setOpenDurationMs(long openDurationMs) { this.openDurationMs = openDurationMs; }
    }
}
