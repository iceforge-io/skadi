package org.iceforge.skadi.semantic.service;

/**
 * A no-op {@link SemanticExecutionMetrics} for tests and unconfigured deployments.
 *
 * <p>All methods are empty. Use {@link #INSTANCE} wherever a {@link SemanticExecutionMetrics}
 * is required but no real recording is needed.
 */
public final class NoOpSemanticExecutionMetrics implements SemanticExecutionMetrics {

    /** Singleton instance — this class has no state. */
    public static final NoOpSemanticExecutionMetrics INSTANCE = new NoOpSemanticExecutionMetrics();

    private NoOpSemanticExecutionMetrics() {}

    @Override public void recordAttempt()       {}
    @Override public void recordCacheHit()      {}
    @Override public void recordAsyncAccepted() {}
    @Override public void recordSuccess()       {}
    @Override public void recordFailure()       {}
    @Override public void recordTimeout()       {}
    @Override public void recordError()         {}
}
