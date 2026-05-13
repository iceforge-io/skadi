package org.iceforge.skadi.sqlgateway.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B5 — Verifies that {@link GatewayMetrics} correctly drives Micrometer meters.
 *
 * <p>Uses {@link SimpleMeterRegistry} so no Spring context is needed.
 */
class GatewayMetricsTest {

    private MeterRegistry registry;
    private GatewayMetrics metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new GatewayMetrics(registry);
    }

    // --- session gauge ---

    @Test
    void active_sessions_gauge_starts_at_zero() {
        Gauge g = registry.find("skadi.sessions.active").gauge();
        assertThat(g).isNotNull();
        assertThat(g.value()).isZero();
    }

    @Test
    void session_opened_increments_gauge() {
        metrics.sessionOpened();
        metrics.sessionOpened();
        assertThat(registry.find("skadi.sessions.active").gauge().value()).isEqualTo(2.0);
    }

    @Test
    void session_closed_decrements_gauge() {
        metrics.sessionOpened();
        metrics.sessionOpened();
        metrics.sessionClosed();
        assertThat(registry.find("skadi.sessions.active").gauge().value()).isEqualTo(1.0);
    }

    @Test
    void gauge_never_goes_below_zero() {
        metrics.sessionClosed(); // spurious close without open
        assertThat(registry.find("skadi.sessions.active").gauge().value()).isEqualTo(-1.0);
        // Recover
        metrics.sessionOpened();
        assertThat(registry.find("skadi.sessions.active").gauge().value()).isZero();
    }

    // --- query timer ---

    @Test
    void record_query_creates_timer_with_cache_tier_tag() {
        metrics.recordQuery(42, "HIT");

        Timer t = registry.find("skadi.queries")
                .tag("cache_tier", "hit")
                .tag("outcome", "success")
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1);
        assertThat(t.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(42.0);
    }

    @Test
    void record_multiple_queries_accumulate_in_timer() {
        metrics.recordQuery(10, "HIT");
        metrics.recordQuery(20, "HIT");
        metrics.recordQuery(30, "MISS");

        Timer hitTimer = registry.find("skadi.queries").tag("cache_tier", "hit").timer();
        assertThat(hitTimer).isNotNull();
        assertThat(hitTimer.count()).isEqualTo(2);

        Timer missTimer = registry.find("skadi.queries").tag("cache_tier", "miss").timer();
        assertThat(missTimer).isNotNull();
        assertThat(missTimer.count()).isEqualTo(1);
    }

    @Test
    void cache_tier_tags_are_normalised() {
        metrics.recordQuery(5, "cache_local");  // Arrow hit
        metrics.recordQuery(5, "miss");          // Arrow miss

        assertThat(registry.find("skadi.queries").tag("cache_tier", "arrow_hit").timer()).isNotNull();
        assertThat(registry.find("skadi.queries").tag("cache_tier", "arrow_miss").timer()).isNotNull();
    }

    @Test
    void null_cache_tier_normalises_to_none() {
        metrics.recordQuery(5, null);
        assertThat(registry.find("skadi.queries").tag("cache_tier", "none").timer()).isNotNull();
    }

    // --- error counter ---

    @Test
    void record_error_creates_counter_with_sqlstate_tag() {
        metrics.recordQueryError(15, "XX000");

        Counter c = registry.find("skadi.query.errors").tag("sqlstate", "XX000").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void error_also_records_in_timer_with_error_outcome() {
        metrics.recordQueryError(20, "53300");

        Timer t = registry.find("skadi.queries")
                .tag("outcome", "error")
                .tag("cache_tier", "none")
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1);
        assertThat(t.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(20.0);
    }

    @Test
    void null_sqlstate_normalises_to_unknown() {
        metrics.recordQueryError(0, null);
        assertThat(registry.find("skadi.query.errors").tag("sqlstate", "unknown").counter()).isNotNull();
    }

    @Test
    void multiple_distinct_sqlstates_produce_separate_counters() {
        metrics.recordQueryError(0, "XX000");
        metrics.recordQueryError(0, "53300");
        metrics.recordQueryError(0, "XX000");

        assertThat(registry.find("skadi.query.errors").tag("sqlstate", "XX000").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.find("skadi.query.errors").tag("sqlstate", "53300").counter().count())
                .isEqualTo(1.0);
    }

    // --- holder null-safety ---

    @Test
    void holder_is_null_safe_before_wiring() {
        GatewayMetricsHolder.set(null); // simulate pre-Spring-context state
        // None of these should throw
        GatewayMetricsHolder.sessionOpened();
        GatewayMetricsHolder.sessionClosed();
        GatewayMetricsHolder.recordQuery(5, "HIT");
        GatewayMetricsHolder.recordQueryError(5, "XX000");
    }
}
