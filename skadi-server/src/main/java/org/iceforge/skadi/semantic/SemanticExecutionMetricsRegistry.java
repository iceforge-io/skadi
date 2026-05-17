package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.service.SemanticExecutionMetrics;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.LongAdder;

/**
 * In-memory counter registry for semantic execution observability (Lane E E2).
 *
 * <p>Implements {@link SemanticExecutionMetrics} using {@link LongAdder} counters
 * and exposes snapshot values for the diagnostics endpoint. Thread-safe.
 *
 * <p>Counts reset on server restart. For persistent metrics, expose these values
 * via Micrometer in a future observability story.
 */
@Component
public class SemanticExecutionMetricsRegistry implements SemanticExecutionMetrics {

    private final LongAdder attempts      = new LongAdder();
    private final LongAdder cacheHits     = new LongAdder();
    private final LongAdder asyncAccepted = new LongAdder();
    private final LongAdder successes     = new LongAdder();
    private final LongAdder failures      = new LongAdder();
    private final LongAdder timeouts      = new LongAdder();
    private final LongAdder errors        = new LongAdder();

    @Override public void recordAttempt()       { attempts.increment(); }
    @Override public void recordCacheHit()      { cacheHits.increment(); }
    @Override public void recordAsyncAccepted() { asyncAccepted.increment(); }
    @Override public void recordSuccess()       { successes.increment(); }
    @Override public void recordFailure()       { failures.increment(); }
    @Override public void recordTimeout()       { timeouts.increment(); }
    @Override public void recordError()         { errors.increment(); }

    public long attempts()      { return attempts.sum(); }
    public long cacheHits()     { return cacheHits.sum(); }
    public long asyncAccepted() { return asyncAccepted.sum(); }
    public long successes()     { return successes.sum(); }
    public long failures()      { return failures.sum(); }
    public long timeouts()      { return timeouts.sum(); }
    public long errors()        { return errors.sum(); }
}
