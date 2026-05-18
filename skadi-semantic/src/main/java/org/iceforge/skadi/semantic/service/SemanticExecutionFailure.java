package org.iceforge.skadi.semantic.service;

/**
 * Classifies semantic execution delegation failures into stable, operator-safe categories.
 *
 * <p>Each value carries a {@link #safeMessage()} that is:
 * <ul>
 *   <li>stable — the string does not change between invocations for the same failure type</li>
 *   <li>safe — contains no secrets, JDBC URLs, credentials, raw exception text, or stack traces</li>
 *   <li>operator-readable — meaningful to a platform operator without internal implementation detail</li>
 * </ul>
 *
 * <p>Raw exception details, HTTP status codes, and server-side error messages are logged
 * at WARN/ERROR level for operator diagnosis but are never propagated to callers or stored
 * in the circuit-breaker reason that appears in diagnostics responses.
 *
 * <p>Use {@link #fromHealthStatus(SemanticExecutionHealthStatus)} to derive the failure
 * classification from a circuit-breaker status at pre-call check time.
 */
public enum SemanticExecutionFailure {

    /** Delegation is disabled by configuration. No HTTP calls were attempted. */
    DISABLED,

    /** Circuit breaker tripped; calls are paused until the open window expires and a probe succeeds. */
    CIRCUIT_OPEN,

    /** Remote server was unreachable, returned an unexpected response, or refused the connection. */
    UNAVAILABLE,

    /** Delegation timed out before the remote server returned a terminal state. */
    TIMEOUT,

    /** Remote server accepted the query but returned a FAILED or CANCELED terminal state. */
    REMOTE_ERROR,

    /** An unexpected exception occurred during delegation. Details in structured logs only. */
    UNEXPECTED;

    /**
     * Returns a stable, operator-safe failure message.
     * Contains no secrets, exception class names, stack traces, JDBC URLs, or credentials.
     */
    public String safeMessage() {
        return switch (this) {
            case DISABLED     -> "semantic execution is disabled";
            case CIRCUIT_OPEN -> "semantic execution circuit is open";
            case UNAVAILABLE  -> "semantic execution server is unavailable";
            case TIMEOUT      -> "semantic execution timed out";
            case REMOTE_ERROR -> "remote server reported a query failure";
            case UNEXPECTED   -> "semantic execution failed unexpectedly";
        };
    }

    /**
     * Derives the failure classification from a circuit-breaker health status.
     * Used on the pre-call path when {@link SemanticExecutionCircuitBreaker#isCallAllowed()} returns false.
     *
     * @param status the current CB health status
     * @return the corresponding failure classification
     */
    public static SemanticExecutionFailure fromHealthStatus(SemanticExecutionHealthStatus status) {
        return switch (status) {
            case DISABLED     -> DISABLED;
            case CIRCUIT_OPEN -> CIRCUIT_OPEN;
            default           -> UNAVAILABLE;
        };
    }
}
