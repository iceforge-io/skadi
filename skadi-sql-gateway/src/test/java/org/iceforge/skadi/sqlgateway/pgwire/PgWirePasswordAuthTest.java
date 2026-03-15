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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for pgwire password authentication.
 *
 * <p>Uses plaintext credential-store with a single user (alice/correct).
 * Verifies correct password connects, wrong password receives 28P01.
 */
@SpringBootTest(
        classes = SkadiSqlGatewayApplication.class,
        properties = {
                "skadi.sql-gateway.pgwire.enabled=true",
                "skadi.sql-gateway.pgwire.host=127.0.0.1",
                "skadi.sql-gateway.pgwire.port=0",
                "skadi.sql-gateway.pgwire.auth.mode=password",
                "skadi.sql-gateway.pgwire.auth.credential-store=plaintext",
                "skadi.sql-gateway.pgwire.auth.users.alice=correct"
        },
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
class PgWirePasswordAuthTest {

    @Autowired
    private PgWireServerLifecycle lifecycle;

    private String baseUrl() {
        int port = lifecycle.getServer().getLocalPort();
        return "jdbc:postgresql://127.0.0.1:" + port
                + "/postgres?ssl=false&socketTimeout=5&connectTimeout=5&preferQueryMode=simple";
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void correctPasswordAllowsConnection() throws Exception {
        Class.forName("org.postgresql.Driver");
        DriverManager.setLoginTimeout(5);

        try (Connection c = DriverManager.getConnection(baseUrl(), "alice", "correct");
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("select 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void wrongPasswordReturns28P01() {
        Class<?> driverClass;
        try { driverClass = Class.forName("org.postgresql.Driver"); } catch (Exception e) { throw new RuntimeException(e); }
        DriverManager.setLoginTimeout(5);

        assertThatThrownBy(() -> DriverManager.getConnection(baseUrl(), "alice", "wrong"))
                .isInstanceOf(java.sql.SQLException.class)
                .satisfies(ex -> assertThat(((java.sql.SQLException) ex).getSQLState()).isEqualTo("28P01"));
    }
}
