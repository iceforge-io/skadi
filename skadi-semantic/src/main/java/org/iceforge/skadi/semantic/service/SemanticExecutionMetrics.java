package org.iceforge.skadi.semantic.service;

/**
 * Observability callback for {@link SkadiServerQueryExecutionService} execution paths.
 *
 * <p>Implementations record counters or metrics for each outcome of a semantic execution
 * attempt. The default no-op implementation is {@link NoOpSemanticExecutionMetrics}.
 *
 * <p>Every call to {@link QueryExecutionService#execute} triggers exactly one
 * {@link #recordAttempt()} followed by exactly one terminal counter:
 * {@link #recordCacheHit()}, {@link #recordSuccess()}, {@link #recordFailure()},
 * {@link #recordTimeout()}, or {@link #recordError()}. Async queries additionally
 * trigger {@link #recordAsyncAccepted()} between attempt and the terminal counter.
 *
 * <p>Implementations must be thread-safe.
 */
public interface SemanticExecutionMetrics {

    /** Called at the start of every {@code execute()} invocation. */
    void recordAttempt();

    /** Called when the submit response is {@code 200 SUCCEEDED} (cache hit on server). */
    void recordCacheHit();

    /**
     * Called when the submit response is {@code 202 ACCEPTED} — the query started
     * asynchronously and polling will follow.
     */
    void recordAsyncAccepted();

    /** Called when a polled query reaches {@code SUCCEEDED} state. */
    void recordSuccess();

    /** Called when a query reaches {@code FAILED} or {@code CANCELED} state, or when
     *  the submit response is an unexpected non-success. */
    void recordFailure();

    /** Called when polling exhausts the max-wait deadline without a terminal state. */
    void recordTimeout();

    /** Called when an unexpected exception propagates from the HTTP layer. */
    void recordError();
}
