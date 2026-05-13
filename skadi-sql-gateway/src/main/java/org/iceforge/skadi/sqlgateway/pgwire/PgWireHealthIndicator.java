package org.iceforge.skadi.sqlgateway.pgwire;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Spring Boot Actuator health indicator for the pgwire listener.
 *
 * <p>Contributes to {@code /actuator/health} when pgwire is enabled, reporting:
 * <ul>
 *   <li>{@code UP} — listener is bound and accepting connections, with the bound port</li>
 *   <li>{@code DOWN} — listener failed to start or has not yet bound</li>
 * </ul>
 *
 * <p>This indicator is only registered when {@code skadi.sql-gateway.pgwire.enabled=true},
 * so health checks in non-pgwire configurations are unaffected.
 *
 * <p>Use this indicator as the k8s readiness probe target:
 * <pre>
 *   readinessProbe:
 *     httpGet:
 *       path: /actuator/health/pgWire
 *       port: 8090
 * </pre>
 */
@Component("pgWire")
@ConditionalOnProperty(prefix = "skadi.sql-gateway.pgwire", name = "enabled", havingValue = "true")
public class PgWireHealthIndicator implements HealthIndicator {

    private final PgWireServerLifecycle lifecycle;

    public PgWireHealthIndicator(PgWireServerLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public Health health() {
        if (!lifecycle.isRunning()) {
            return Health.down()
                    .withDetail("reason", "pgwire disabled or startup failed")
                    .build();
        }
        PgWireServer server = lifecycle.getServer();
        if (server == null) {
            return Health.down()
                    .withDetail("reason", "pgwire server instance unavailable")
                    .build();
        }
        int port = server.getLocalPort();
        if (port <= 0) {
            return Health.down()
                    .withDetail("reason", "pgwire server not yet bound to a port")
                    .build();
        }
        return Health.up()
                .withDetail("port", port)
                .withDetail("protocol", "pgwire/PostgreSQL")
                .build();
    }
}
