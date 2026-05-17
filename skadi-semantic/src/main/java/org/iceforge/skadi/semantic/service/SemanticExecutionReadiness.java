package org.iceforge.skadi.semantic.service;

/**
 * Operational readiness classification for the semantic execution delegation path.
 *
 * <p>Maps the fine-grained {@link SemanticExecutionHealthStatus} values to four
 * operator-facing categories suitable for dashboards, alerting, and deployment gates.
 *
 * <table border="1">
 *   <tr><th>Health status</th><th>Readiness</th><th>Meaning</th></tr>
 *   <tr><td>HEALTHY</td><td>READY</td><td>Path is healthy; queries will be delegated.</td></tr>
 *   <tr><td>DISABLED</td><td>DISABLED</td><td>Disabled by configuration; no calls attempted.</td></tr>
 *   <tr><td>CIRCUIT_OPEN</td><td>DEGRADED</td><td>Circuit tripped; auto-probe pending.</td></tr>
 *   <tr><td>UNAVAILABLE</td><td>UNAVAILABLE</td><td>Server unreachable or unexpected response.</td></tr>
 *   <tr><td>TIMEOUT</td><td>UNAVAILABLE</td><td>Server did not respond within the wait window.</td></tr>
 *   <tr><td>FAILED</td><td>UNAVAILABLE</td><td>Server returned a terminal failure state.</td></tr>
 * </table>
 */
public enum SemanticExecutionReadiness {

    /** Path is healthy; semantic query delegation will proceed normally. */
    READY,

    /** Delegation is disabled by configuration ({@code skadi.semantic.execution.enabled=false}). */
    DISABLED,

    /**
     * Circuit breaker has tripped due to consecutive failures; calls are short-circuited
     * until the open window expires and a probe call succeeds.
     */
    DEGRADED,

    /**
     * Server is unreachable, unresponsive, or returning errors; calls fail immediately.
     * If the circuit breaker is enabled, repeated failures will transition to {@link #DEGRADED}.
     */
    UNAVAILABLE;

    /**
     * Maps a {@link SemanticExecutionHealthStatus} to its operational readiness classification.
     *
     * @param status the current health status from the circuit breaker
     * @return the corresponding readiness category
     */
    public static SemanticExecutionReadiness from(SemanticExecutionHealthStatus status) {
        return switch (status) {
            case HEALTHY      -> READY;
            case DISABLED     -> DISABLED;
            case CIRCUIT_OPEN -> DEGRADED;
            case UNAVAILABLE, TIMEOUT, FAILED -> UNAVAILABLE;
        };
    }
}
