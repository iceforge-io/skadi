package org.iceforge.skadi.sqlgateway.metrics;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Static bridge that lets {@code PgWireSession} (manually constructed, outside Spring)
 * record metrics without constructor injection.
 *
 * <p>All public methods are null-safe — they silently no-op when no {@link GatewayMetrics}
 * has been wired in (e.g. in unit tests that don't start a Spring context).
 */
public final class GatewayMetricsHolder {

    private static final AtomicReference<GatewayMetrics> REF = new AtomicReference<>();

    private GatewayMetricsHolder() {}

    public static void set(GatewayMetrics metrics) {
        REF.set(metrics);
    }

    public static void sessionOpened() {
        GatewayMetrics m = REF.get();
        if (m != null) m.sessionOpened();
    }

    public static void sessionClosed() {
        GatewayMetrics m = REF.get();
        if (m != null) m.sessionClosed();
    }

    public static void recordQuery(long latencyMs, String cacheTier) {
        GatewayMetrics m = REF.get();
        if (m != null) m.recordQuery(latencyMs, cacheTier);
    }

    public static void recordQueryError(long latencyMs, String sqlState) {
        GatewayMetrics m = REF.get();
        if (m != null) m.recordQueryError(latencyMs, sqlState);
    }
}
