package org.iceforge.skadi.sqlgateway.metrics;

import org.springframework.context.annotation.Configuration;

/**
 * Wires the Spring-managed {@link GatewayMetrics} bean into the static
 * {@link GatewayMetricsHolder} so that manually-constructed {@code PgWireSession}
 * objects can record metrics without constructor injection.
 */
@Configuration
class GatewayMetricsWiring {

    GatewayMetricsWiring(GatewayMetrics metrics) {
        GatewayMetricsHolder.set(metrics);
    }
}
