package org.iceforge.skadi.semantic.service;

/**
 * Health/readiness classification for the semantic execution delegation path.
 *
 * <p>Used by {@link SemanticExecutionCircuitBreaker} to report the current state
 * of the {@link SkadiServerQueryExecutionService} delegation path.
 */
public enum SemanticExecutionHealthStatus {

    /** Delegation is disabled via configuration. No calls are attempted. */
    DISABLED,

    /** Last delegation attempt succeeded; path is considered healthy. */
    HEALTHY,

    /** Server was unreachable or returned an unexpected response. */
    UNAVAILABLE,

    /** Delegation timed out before a terminal state was reached. */
    TIMEOUT,

    /** Server returned a FAILED or CANCELED terminal state. */
    FAILED,

    /**
     * Failure threshold exceeded; circuit is open and calls are short-circuited
     * until the open window expires.
     */
    CIRCUIT_OPEN
}
