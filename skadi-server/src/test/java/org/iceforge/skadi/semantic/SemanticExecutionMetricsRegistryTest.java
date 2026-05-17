package org.iceforge.skadi.semantic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link SemanticExecutionMetricsRegistry}.
 * No Spring context — plain instantiation.
 */
class SemanticExecutionMetricsRegistryTest {

    @Test
    void counters_startAtZero() {
        var reg = new SemanticExecutionMetricsRegistry();
        assertEquals(0, reg.attempts());
        assertEquals(0, reg.cacheHits());
        assertEquals(0, reg.asyncAccepted());
        assertEquals(0, reg.successes());
        assertEquals(0, reg.failures());
        assertEquals(0, reg.timeouts());
        assertEquals(0, reg.errors());
    }

    @Test
    void recordAttempt_incrementsAttempts() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordAttempt();
        reg.recordAttempt();
        assertEquals(2, reg.attempts());
    }

    @Test
    void recordCacheHit_incrementsCacheHits() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordCacheHit();
        assertEquals(1, reg.cacheHits());
        assertEquals(0, reg.successes());
    }

    @Test
    void recordAsyncAccepted_incrementsAsyncAccepted() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordAsyncAccepted();
        assertEquals(1, reg.asyncAccepted());
    }

    @Test
    void recordSuccess_incrementsSuccesses() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordSuccess();
        assertEquals(1, reg.successes());
        assertEquals(0, reg.failures());
    }

    @Test
    void recordFailure_incrementsFailures() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordFailure();
        assertEquals(1, reg.failures());
        assertEquals(0, reg.successes());
    }

    @Test
    void recordTimeout_incrementsTimeouts() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordTimeout();
        assertEquals(1, reg.timeouts());
    }

    @Test
    void recordError_incrementsErrors() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordError();
        assertEquals(1, reg.errors());
    }

    @Test
    void counters_areIndependent() {
        var reg = new SemanticExecutionMetricsRegistry();
        reg.recordAttempt();
        reg.recordCacheHit();
        assertEquals(1, reg.attempts());
        assertEquals(1, reg.cacheHits());
        assertEquals(0, reg.asyncAccepted());
        assertEquals(0, reg.successes());
        assertEquals(0, reg.failures());
        assertEquals(0, reg.timeouts());
        assertEquals(0, reg.errors());
    }
}
