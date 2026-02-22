package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.SkadiSqlGatewayApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = SkadiSqlGatewayApplication.class,
        properties = {
                "skadi.sql-gateway.pgwire.enabled=true",
                "skadi.sql-gateway.pgwire.host=127.0.0.1",
                "skadi.sql-gateway.pgwire.port=0",
                "skadi.sql-gateway.pgwire.auth.mode=trust"
        },
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
class PgWireServerTest {

    @Autowired
    private PgWireServerLifecycle lifecycle;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void jdbcClientCanConnectAndSelect1() throws Exception {
        // Defensive: ensure driver is loaded even if META-INF/services discovery doesn't run.
        Class.forName("org.postgresql.Driver");

        PgWireServer server = lifecycle.getServer();
        assertThat(server).isNotNull();
        int port = server.getLocalPort();
        assertThat(port).isGreaterThan(0);

        // Bound how long the driver can hang during connect/handshake.
        DriverManager.setLoginTimeout(5);

        String url = "jdbc:postgresql://127.0.0.1:" + port + "/postgres?ssl=false&socketTimeout=5&connectTimeout=5";
        // Force simple query protocol to keep the MVP pgwire server scope small.
        url += "&preferQueryMode=simple";
        try (Connection c = DriverManager.getConnection(url, "test", "ignored");
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("select 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }
}
