package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.iceforge.skadi.semantic.service.SemanticExecutionCircuitBreaker;
import org.iceforge.skadi.semantic.service.SemanticExecutionHealthSnapshot;
import org.iceforge.skadi.semantic.service.SemanticExecutionHealthStatus;
import org.iceforge.skadi.semantic.service.SemanticExecutionReadiness;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * Read-only diagnostics endpoint for the semantic execution delegation path.
 *
 * <h2>Endpoint</h2>
 * <pre>GET /api/semantic/v1/execution/status</pre>
 *
 * <p>Returns the current activation state, configuration (server URL, datasource ID — no
 * credentials), lifetime execution counters, and circuit-breaker health snapshot.
 * Useful for confirming the path is wired and healthy before depending on it.
 *
 * <p>This endpoint does not execute queries, call Databricks, or modify any state.
 */
@RestController
@RequestMapping("/api/semantic/v1/execution")
public class SemanticExecutionDiagnosticsController {

    private final SemanticExecutionProperties execProps;
    private final SemanticExecutionMetricsRegistry metricsRegistry;
    private final SemanticExecutionCircuitBreaker circuitBreaker;

    public SemanticExecutionDiagnosticsController(
            SemanticExecutionProperties execProps,
            SemanticExecutionMetricsRegistry metricsRegistry,
            SemanticExecutionCircuitBreaker circuitBreaker) {
        this.execProps       = execProps;
        this.metricsRegistry = metricsRegistry;
        this.circuitBreaker  = circuitBreaker;
    }

    @GetMapping(value = "/status", produces = "application/json")
    public ExecutionStatusResponse status() {
        SemanticExecutionHealthSnapshot snap = circuitBreaker.snapshot();
        boolean active = snap.status() != SemanticExecutionHealthStatus.DISABLED;
        return new ExecutionStatusResponse(
                active,
                execProps.getServerUrl(),
                execProps.getDatasourceId(),
                new MetricsSnapshot(
                        metricsRegistry.attempts(),
                        metricsRegistry.cacheHits(),
                        metricsRegistry.asyncAccepted(),
                        metricsRegistry.successes(),
                        metricsRegistry.failures(),
                        metricsRegistry.timeouts(),
                        metricsRegistry.errors()),
                new HealthSnapshot(
                        snap.status().name(),
                        SemanticExecutionReadiness.from(snap.status()).name(),
                        snap.failureCount(),
                        snap.failureThreshold(),
                        snap.lastSuccessAt(),
                        snap.lastFailureAt(),
                        snap.lastFailureReason(),
                        snap.circuitOpenUntil()));
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ExecutionStatusResponse(
            boolean active,
            String serverUrl,
            String datasourceId,
            MetricsSnapshot metrics,
            HealthSnapshot health) {}

    public record MetricsSnapshot(
            long attempts,
            long cacheHits,
            long asyncAccepted,
            long successes,
            long failures,
            long timeouts,
            long errors) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record HealthSnapshot(
            String status,
            String readiness,
            int failureCount,
            int failureThreshold,
            Instant lastSuccessAt,
            Instant lastFailureAt,
            String lastFailureReason,
            Instant circuitOpenUntil) {}
}
