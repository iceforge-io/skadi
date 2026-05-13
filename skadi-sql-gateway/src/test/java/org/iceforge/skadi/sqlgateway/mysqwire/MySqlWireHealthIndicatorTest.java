package org.iceforge.skadi.sqlgateway.mysqwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B8 — Verifies {@link MySqlWireHealthIndicator} lifecycle states.
 */
class MySqlWireHealthIndicatorTest {

    @Test
    void reports_up_when_server_is_running() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.MySqlWire props =
                new SqlGatewayProperties.MySqlWire(true, "127.0.0.1", 0, auth, null, null);

        try (MySqlWireServer server = new MySqlWireServer(props, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            MySqlWireServerLifecycle lifecycle = stubLifecycle(props, server);
            MySqlWireHealthIndicator indicator = new MySqlWireHealthIndicator(lifecycle);

            Health health = indicator.health();

            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsKey("port");
            assertThat((Integer) health.getDetails().get("port")).isGreaterThan(0);
            assertThat(health.getDetails()).containsEntry("protocol", "mysql/MySQL-wire");
        }
    }

    @Test
    void reports_down_when_lifecycle_not_running() {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.MySqlWire props =
                new SqlGatewayProperties.MySqlWire(false, "127.0.0.1", 13306, auth, null, null);

        SqlGatewayProperties gwProps =
                new SqlGatewayProperties(null, null, null, null, null, null,
                        new SqlGatewayProperties.MySqlWire(false, "127.0.0.1", 13306, auth, null, null));
        MySqlWireServerLifecycle lifecycle = new MySqlWireServerLifecycle(gwProps);
        // Do not start — simulates disabled state

        MySqlWireHealthIndicator indicator = new MySqlWireHealthIndicator(lifecycle);
        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsKey("reason");
    }

    // --- helpers ---

    private static MySqlWireServerLifecycle stubLifecycle(SqlGatewayProperties.MySqlWire props,
                                                           MySqlWireServer server) {
        SqlGatewayProperties gwProps =
                new SqlGatewayProperties(null, null, null, null, null, null, props);
        MySqlWireServerLifecycle lifecycle = new MySqlWireServerLifecycle(gwProps);
        try {
            java.lang.reflect.Field f = MySqlWireServerLifecycle.class.getDeclaredField("server");
            f.setAccessible(true);
            f.set(lifecycle, server);
            java.lang.reflect.Field r = MySqlWireServerLifecycle.class.getDeclaredField("running");
            r.setAccessible(true);
            r.set(lifecycle, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return lifecycle;
    }
}
