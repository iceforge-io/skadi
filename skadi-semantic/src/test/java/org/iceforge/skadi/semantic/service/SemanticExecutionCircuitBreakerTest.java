package org.iceforge.skadi.semantic.service;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SemanticExecutionCircuitBreaker}.
 * No Spring context, no HTTP, no external services.
 */
class SemanticExecutionCircuitBreakerTest {

    private static final String URL = "http://localhost:8080";
    private static final int THRESHOLD = 3;
    private static final long OPEN_MS = 1_000L;

    private static SemanticExecutionCircuitBreaker breaker(Clock clock) {
        return new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, URL, clock);
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.parse("2026-05-17T10:00:00Z"), ZoneOffset.UTC);
    }

    // ── Disabled state ────────────────────────────────────────────────────────

    @Test
    void disabled_isCallAllowed_returnsFalse() {
        var cb = SemanticExecutionCircuitBreaker.disabled(URL);
        assertFalse(cb.isCallAllowed());
    }

    @Test
    void disabled_snapshot_statusIsDisabled() {
        var cb = SemanticExecutionCircuitBreaker.disabled(URL);
        assertEquals(SemanticExecutionHealthStatus.DISABLED, cb.snapshot().status());
    }

    @Test
    void disabled_recordSuccess_doesNotChangeToHealthy() {
        var cb = SemanticExecutionCircuitBreaker.disabled(URL);
        cb.recordSuccess();
        // Disabled means disabled — success cannot re-enable the circuit
        assertFalse(cb.isCallAllowed());
        assertEquals(SemanticExecutionHealthStatus.DISABLED, cb.snapshot().status());
    }

    // ── Healthy (initial) state ───────────────────────────────────────────────

    @Test
    void healthy_isCallAllowed_returnsTrue() {
        assertTrue(breaker(fixedClock()).isCallAllowed());
    }

    @Test
    void healthy_snapshot_statusIsHealthy() {
        assertEquals(SemanticExecutionHealthStatus.HEALTHY, breaker(fixedClock()).snapshot().status());
    }

    @Test
    void healthy_snapshot_serverUrlIsExposed() {
        assertEquals(URL, breaker(fixedClock()).snapshot().skadiServerBaseUrl());
    }

    // ── Success recording ─────────────────────────────────────────────────────

    @Test
    void recordSuccess_resetsFailureCount() {
        Clock clock = fixedClock();
        var cb = breaker(clock);
        cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        cb.recordSuccess();
        assertEquals(0, cb.snapshot().failureCount());
        assertEquals(SemanticExecutionHealthStatus.HEALTHY, cb.snapshot().status());
    }

    @Test
    void recordSuccess_setsLastSuccessAt() {
        Clock clock = fixedClock();
        var cb = breaker(clock);
        cb.recordSuccess();
        assertEquals(Instant.now(clock), cb.snapshot().lastSuccessAt());
    }

    // ── Failure recording ─────────────────────────────────────────────────────

    @Test
    void recordFailure_incrementsFailureCount() {
        var cb = breaker(fixedClock());
        cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        assertEquals(1, cb.snapshot().failureCount());
    }

    @Test
    void recordFailure_belowThreshold_setsGivenStatus() {
        var cb = breaker(fixedClock());
        cb.recordFailure("err", SemanticExecutionHealthStatus.TIMEOUT);
        assertEquals(SemanticExecutionHealthStatus.TIMEOUT, cb.snapshot().status());
        assertTrue(cb.isCallAllowed(), "below threshold — circuit must remain closed");
    }

    @Test
    void recordFailure_setsLastFailureAtAndReason() {
        Clock clock = fixedClock();
        var cb = breaker(clock);
        cb.recordFailure("server down", SemanticExecutionHealthStatus.UNAVAILABLE);
        assertEquals(Instant.now(clock), cb.snapshot().lastFailureAt());
        assertEquals("server down", cb.snapshot().lastFailureReason());
    }

    // ── Circuit opening ───────────────────────────────────────────────────────

    @Test
    void thresholdFailures_openCircuit() {
        var cb = breaker(fixedClock());
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        assertEquals(SemanticExecutionHealthStatus.CIRCUIT_OPEN, cb.snapshot().status());
    }

    @Test
    void circuitOpen_isCallAllowed_returnsFalse() {
        var cb = breaker(fixedClock());
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        assertFalse(cb.isCallAllowed());
    }

    @Test
    void circuitOpen_circuitOpenUntil_isSet() {
        Clock clock = fixedClock();
        var cb = breaker(clock);
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        Instant expected = Instant.now(clock).plusMillis(OPEN_MS);
        assertEquals(expected, cb.snapshot().circuitOpenUntil());
    }

    // ── Circuit self-healing (half-open probe) ────────────────────────────────

    @Test
    void circuitOpen_afterWindowExpires_allowsProbeCall() {
        Instant base = Instant.parse("2026-05-17T10:00:00Z");
        // Use a mutable clock reference via AtomicReference trick with a holder array
        Instant[] now = {base};
        Clock clock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now[0]; }
        };

        var cb = new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, URL, clock);
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        assertFalse(cb.isCallAllowed(), "circuit must be closed immediately after tripping");

        // Advance past open window
        now[0] = base.plusMillis(OPEN_MS + 1);
        assertTrue(cb.isCallAllowed(), "circuit must allow probe after window expires");
        assertEquals(SemanticExecutionHealthStatus.HEALTHY, cb.snapshot().status());
    }

    @Test
    void circuitOpen_probeSucceeds_resetsCircuit() {
        Instant base = Instant.parse("2026-05-17T10:00:00Z");
        Instant[] now = {base};
        Clock clock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now[0]; }
        };

        var cb = new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, URL, clock);
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        now[0] = base.plusMillis(OPEN_MS + 1);
        cb.isCallAllowed(); // probe
        cb.recordSuccess();

        assertEquals(SemanticExecutionHealthStatus.HEALTHY, cb.snapshot().status());
        assertEquals(0, cb.snapshot().failureCount());
        assertNull(cb.snapshot().circuitOpenUntil());
        assertTrue(cb.isCallAllowed());
    }

    @Test
    void circuitOpen_probeFails_reOpensCircuit() {
        Instant base = Instant.parse("2026-05-17T10:00:00Z");
        Instant[] now = {base};
        Clock clock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now[0]; }
        };

        var cb = new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, URL, clock);
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        now[0] = base.plusMillis(OPEN_MS + 1);
        cb.isCallAllowed(); // probe → transitions to HEALTHY
        cb.recordFailure("still down", SemanticExecutionHealthStatus.UNAVAILABLE); // count = THRESHOLD
        // failureCount was 3 (at threshold), recordFailure makes it 4 >= 3 → re-open
        assertEquals(SemanticExecutionHealthStatus.CIRCUIT_OPEN, cb.snapshot().status());
        assertFalse(cb.isCallAllowed());
    }

    // ── alwaysAllow factory ───────────────────────────────────────────────────

    @Test
    void alwaysAllow_neverOpensCircuit() {
        var cb = SemanticExecutionCircuitBreaker.alwaysAllow(URL);
        for (int i = 0; i < 1000; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        assertTrue(cb.isCallAllowed(), "alwaysAllow circuit must never open");
    }

    // ── Snapshot fields ───────────────────────────────────────────────────────

    @Test
    void snapshot_includesThreshold() {
        var cb = breaker(fixedClock());
        assertEquals(THRESHOLD, cb.snapshot().failureThreshold());
    }

    @Test
    void snapshot_initiallyNoTimestamps() {
        var cb = breaker(fixedClock());
        assertNull(cb.snapshot().lastSuccessAt());
        assertNull(cb.snapshot().lastFailureAt());
        assertNull(cb.snapshot().lastFailureReason());
        assertNull(cb.snapshot().circuitOpenUntil());
    }
}
