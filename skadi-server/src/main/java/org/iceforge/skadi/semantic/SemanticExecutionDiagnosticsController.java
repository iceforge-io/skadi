package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only diagnostics endpoint for the Lane E semantic execution path (E2).
 *
 * <h2>Endpoint</h2>
 * <pre>GET /api/semantic/v1/execution/status</pre>
 *
 * <p>Returns whether semantic execution is active, the configured server URL and
 * datasource ID, and lifetime execution counters. Useful for confirming that the
 * execution path is wired correctly and observing basic traffic patterns.
 *
 * <p>This endpoint does not execute queries, call Databricks, or modify any state.
 * Counter values reset on server restart.
 */
@RestController
@RequestMapping("/api/semantic/v1/execution")
public class SemanticExecutionDiagnosticsController {

    private final SemanticExecutionProperties execProps;
    private final SemanticExecutionMetricsRegistry metricsRegistry;

    public SemanticExecutionDiagnosticsController(
            SemanticExecutionProperties execProps,
            SemanticExecutionMetricsRegistry metricsRegistry) {
        this.execProps       = execProps;
        this.metricsRegistry = metricsRegistry;
    }

    /**
     * Returns semantic execution activation status and lifetime counters.
     *
     * <p>Always returns HTTP 200. {@code active} is {@code true} whenever this
     * endpoint is reachable — it confirms the execution path is wired into the
     * server. The {@code serverUrl} and {@code datasourceId} fields show the
     * configured delegation target (no credentials are exposed).
     */
    @GetMapping(value = "/status", produces = "application/json")
    public ExecutionStatusResponse status() {
        return new ExecutionStatusResponse(
                true,
                execProps.getServerUrl(),
                execProps.getDatasourceId(),
                new MetricsSnapshot(
                        metricsRegistry.attempts(),
                        metricsRegistry.cacheHits(),
                        metricsRegistry.asyncAccepted(),
                        metricsRegistry.successes(),
                        metricsRegistry.failures(),
                        metricsRegistry.timeouts(),
                        metricsRegistry.errors()));
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ExecutionStatusResponse(
            boolean active,
            String serverUrl,
            String datasourceId,
            MetricsSnapshot metrics) {}

    public record MetricsSnapshot(
            long attempts,
            long cacheHits,
            long asyncAccepted,
            long successes,
            long failures,
            long timeouts,
            long errors) {}
}
