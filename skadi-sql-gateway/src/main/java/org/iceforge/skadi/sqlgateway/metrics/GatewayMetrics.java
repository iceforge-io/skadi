package org.iceforge.skadi.sqlgateway.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central Micrometer instrumentation for the SQL gateway.
 *
 * <p>Metrics exposed:
 * <ul>
 *   <li>{@code skadi.sessions.active} — gauge: currently connected pgwire clients</li>
 *   <li>{@code skadi.queries} — timer histogram: query latency; tags: {@code cache_tier}, {@code outcome}</li>
 *   <li>{@code skadi.query.errors} — counter: failed queries; tag: {@code sqlstate}</li>
 * </ul>
 *
 * <p>All recording methods are null-safe when called through {@link GatewayMetricsHolder}.
 */
@Component
public final class GatewayMetrics {

    private final MeterRegistry registry;
    private final AtomicLong activeSessions = new AtomicLong();

    public GatewayMetrics(MeterRegistry registry) {
        this.registry = registry;

        Gauge.builder("skadi.sessions.active", activeSessions, AtomicLong::doubleValue)
                .description("Currently active pgwire sessions")
                .register(registry);
    }

    /** Increment active session gauge. */
    public void sessionOpened() {
        activeSessions.incrementAndGet();
    }

    /** Decrement active session gauge. */
    public void sessionClosed() {
        activeSessions.decrementAndGet();
    }

    /**
     * Records a completed query.
     *
     * @param latencyMs  wall-clock duration of the query
     * @param cacheTier  tag value — one of {@code "HIT"}, {@code "MISS"}, {@code "SKIP"},
     *                   {@code "cache_local"}, or {@code "miss"} (Arrow path)
     */
    public void recordQuery(long latencyMs, String cacheTier) {
        timer("success", cacheTier).record(latencyMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Records a failed query.
     *
     * @param latencyMs wall-clock duration before the error was raised
     * @param sqlState  five-character SQLSTATE code (e.g. {@code "XX000"}, {@code "53300"})
     */
    public void recordQueryError(long latencyMs, String sqlState) {
        timer("error", "none").record(latencyMs, TimeUnit.MILLISECONDS);
        Counter.builder("skadi.query.errors")
                .description("Queries that returned an error response")
                .tag("sqlstate", sqlState == null ? "unknown" : sqlState)
                .register(registry)
                .increment();
    }

    // --- internals ---

    private Timer timer(String outcome, String cacheTier) {
        return Timer.builder("skadi.queries")
                .description("Query execution latency")
                .tag("outcome", outcome)
                .tag("cache_tier", normalise(cacheTier))
                .publishPercentileHistogram()
                .register(registry);
    }

    private static String normalise(String tier) {
        if (tier == null) return "none";
        return switch (tier) {
            case "HIT"          -> "hit";
            case "MISS"         -> "miss";
            case "SKIP"         -> "skip";
            case "cache_local"  -> "arrow_hit";
            case "miss"         -> "arrow_miss";   // arrow path miss tag
            default             -> tier.toLowerCase();
        };
    }
}
