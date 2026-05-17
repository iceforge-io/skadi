package org.iceforge.skadi.semantic.service;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/**
 * Simple circuit-breaker for the semantic execution delegation path.
 *
 * <p>Tracks consecutive failures against {@link #failureThreshold}. When the threshold
 * is exceeded the circuit opens and subsequent {@link #isCallAllowed()} calls return
 * {@code false} until the open window ({@link #openDurationMs}) expires. After expiry a
 * single probe call is allowed; a successful probe resets the circuit; a failed probe
 * re-opens it.
 *
 * <p>When constructed with {@code enabled=false} all calls are blocked and the status
 * is permanently {@link SemanticExecutionHealthStatus#DISABLED}.
 *
 * <p>Thread-safe — all state mutations are {@code synchronized}.
 *
 * <p>Accepts a {@link Clock} for deterministic time control in tests.
 */
public final class SemanticExecutionCircuitBreaker {

    private final boolean enabled;
    private final int failureThreshold;
    private final long openDurationMs;
    private final String serverBaseUrl;
    private final Clock clock;

    // Mutable state — guarded by this
    private SemanticExecutionHealthStatus status;
    private int failureCount = 0;
    private Instant lastSuccessAt = null;
    private Instant lastFailureAt = null;
    private String lastFailureReason = null;
    private Instant circuitOpenUntil = null;

    /**
     * @param enabled          whether delegation is active; {@code false} permanently disables calls
     * @param failureThreshold consecutive failures before opening the circuit
     * @param openDurationMs   how long the circuit stays open after tripping (milliseconds)
     * @param serverBaseUrl    base URL of the target skadi-server; included in snapshots
     * @param clock            clock used for {@link Instant#now()} — inject a fixed clock in tests
     */
    public SemanticExecutionCircuitBreaker(
            boolean enabled,
            int failureThreshold,
            long openDurationMs,
            String serverBaseUrl,
            Clock clock) {
        this.enabled = enabled;
        this.failureThreshold = failureThreshold;
        this.openDurationMs = openDurationMs;
        this.serverBaseUrl = Objects.requireNonNull(serverBaseUrl, "serverBaseUrl");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.status = enabled
                ? SemanticExecutionHealthStatus.HEALTHY
                : SemanticExecutionHealthStatus.DISABLED;
    }

    /**
     * Factory for tests that do not exercise circuit-breaker logic.
     * Always allows calls; threshold is set to {@link Integer#MAX_VALUE}.
     */
    public static SemanticExecutionCircuitBreaker alwaysAllow(String serverBaseUrl) {
        return new SemanticExecutionCircuitBreaker(
                true, Integer.MAX_VALUE, 0L, serverBaseUrl, Clock.systemUTC());
    }

    /**
     * Factory for disabled semantic execution (status = {@link SemanticExecutionHealthStatus#DISABLED}).
     */
    public static SemanticExecutionCircuitBreaker disabled(String serverBaseUrl) {
        return new SemanticExecutionCircuitBreaker(
                false, 3, 30_000L, serverBaseUrl, Clock.systemUTC());
    }

    /**
     * Returns {@code true} if a delegation call is permitted right now.
     *
     * <p>When the circuit is open and the open window has expired, transitions the
     * circuit to {@link SemanticExecutionHealthStatus#HEALTHY} and allows one probe call.
     */
    public synchronized boolean isCallAllowed() {
        if (!enabled) return false;
        if (status == SemanticExecutionHealthStatus.CIRCUIT_OPEN) {
            if (clock.instant().isAfter(circuitOpenUntil)) {
                // Open window expired — allow probe call (half-open transition)
                status = SemanticExecutionHealthStatus.HEALTHY;
                circuitOpenUntil = null;
                return true;
            }
            return false;
        }
        return true;
    }

    /**
     * Records a successful delegation. Resets failure count and marks the path HEALTHY.
     * No-op when the circuit is permanently disabled.
     */
    public synchronized void recordSuccess() {
        if (!enabled) return;
        failureCount = 0;
        status = SemanticExecutionHealthStatus.HEALTHY;
        lastSuccessAt = clock.instant();
        circuitOpenUntil = null;
    }

    /**
     * Records a failed delegation. Increments the failure count and opens the circuit
     * if the threshold is reached.
     *
     * @param reason        user-safe description of the failure; must not be null
     * @param failureStatus the specific failure classification to apply if below threshold
     */
    public synchronized void recordFailure(String reason, SemanticExecutionHealthStatus failureStatus) {
        failureCount++;
        lastFailureAt = clock.instant();
        lastFailureReason = reason;
        if (failureCount >= failureThreshold) {
            status = SemanticExecutionHealthStatus.CIRCUIT_OPEN;
            circuitOpenUntil = clock.instant().plusMillis(openDurationMs);
        } else {
            status = failureStatus;
        }
    }

    /** Returns a point-in-time snapshot of current circuit state. */
    public synchronized SemanticExecutionHealthSnapshot snapshot() {
        return new SemanticExecutionHealthSnapshot(
                status,
                serverBaseUrl,
                failureCount,
                failureThreshold,
                lastSuccessAt,
                lastFailureAt,
                lastFailureReason,
                circuitOpenUntil);
    }
}
