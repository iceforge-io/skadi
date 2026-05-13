package org.iceforge.skadi.sqlgateway.mysqwire;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Reports the MySQL wire server's readiness under {@code /actuator/health/mySqlWire}.
 *
 * <p>Active only when {@code skadi.sql-gateway.mysqlwire.enabled=true}.
 */
@Component("mySqlWire")
@ConditionalOnProperty(prefix = "skadi.sql-gateway.mysqlwire", name = "enabled", havingValue = "true")
public class MySqlWireHealthIndicator implements HealthIndicator {

    private final MySqlWireServerLifecycle lifecycle;

    public MySqlWireHealthIndicator(MySqlWireServerLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public Health health() {
        if (!lifecycle.isRunning()) {
            return Health.down()
                    .withDetail("reason", "mysql wire server disabled or startup failed")
                    .build();
        }
        MySqlWireServer server = lifecycle.getServer();
        if (server == null) {
            return Health.down().withDetail("reason", "server not initialised").build();
        }
        int port = server.getLocalPort();
        if (port <= 0) {
            return Health.down().withDetail("reason", "server not yet bound").build();
        }
        return Health.up()
                .withDetail("port", port)
                .withDetail("protocol", "mysql/MySQL-wire")
                .build();
    }
}
