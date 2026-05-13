package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B7 — Verifies that {@link PgWireHealthIndicator} reports correct health status
 * based on the pgwire server lifecycle state.
 */
class PgWireHealthIndicatorTest {

    @Test
    void reports_up_when_server_is_running() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth,
                        null, null, null, null, null);

        // Create and start a real server so getLocalPort() > 0.
        try (PgWireServer server = new PgWireServer(props, null, null, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            PgWireServerLifecycle lifecycle = stubLifecycle(props, server);
            PgWireHealthIndicator indicator = new PgWireHealthIndicator(lifecycle);

            Health health = indicator.health();

            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsKey("port");
            assertThat((Integer) health.getDetails().get("port")).isGreaterThan(0);
            assertThat(health.getDetails()).containsEntry("protocol", "pgwire/PostgreSQL");
        }
    }

    @Test
    void reports_down_when_lifecycle_is_not_running() {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(false, "127.0.0.1", 15432, auth,
                        null, null, null, null, null);

        // Lifecycle that reports not-running (pgwire.enabled=false).
        PgWireServerLifecycle lifecycle = new PgWireServerLifecycle(
                new SqlGatewayProperties(null, null, props, null, null, null));
        // Do not call lifecycle.start() — simulates disabled state.

        PgWireHealthIndicator indicator = new PgWireHealthIndicator(lifecycle);
        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsKey("reason");
    }

    // --- helpers ---

    /** Wraps a started PgWireServer in a minimal lifecycle stub that reports isRunning=true. */
    private static PgWireServerLifecycle stubLifecycle(SqlGatewayProperties.PgWire props,
                                                        PgWireServer server) {
        SqlGatewayProperties.PgWire enabledProps =
                new SqlGatewayProperties.PgWire(true, props.host(), props.port(), props.auth(),
                        null, null, null, null, null);
        PgWireServerLifecycle lifecycle = new PgWireServerLifecycle(
                new SqlGatewayProperties(null, null, enabledProps, null, null, null));
        // Reflectively set the server field so the lifecycle reports the live server.
        try {
            java.lang.reflect.Field f = PgWireServerLifecycle.class.getDeclaredField("server");
            f.setAccessible(true);
            f.set(lifecycle, server);
            java.lang.reflect.Field r = PgWireServerLifecycle.class.getDeclaredField("running");
            r.setAccessible(true);
            r.set(lifecycle, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return lifecycle;
    }
}
