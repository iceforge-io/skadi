package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * B6 — Verifies that the {@code tls.require-ssl} flag causes the gateway to reject
 * clients that connect without sending {@code SSLRequest} first.
 */
class RequireSslTest {

    @Test
    void require_ssl_rejects_plain_connection_with_sqlstate_28000() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire.Tls tls =
                new SqlGatewayProperties.PgWire.Tls(false, true, null, null, null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth,
                        null, null, null, null, tls);

        try (PgWireServer server = new PgWireServer(props, null, null, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            int port = server.getLocalPort();
            Class.forName("org.postgresql.Driver");

            // sslmode=disable tells the driver to skip SSLRequest entirely and send
            // StartupMessage directly. The server must reject that with SQLSTATE 28000.
            // (ssl=false alone does not suppress SSLRequest in some JDBC driver versions.)
            String url = "jdbc:postgresql://127.0.0.1:" + port
                    + "/postgres?sslmode=disable&socketTimeout=5";
            assertThatThrownBy(() -> DriverManager.getConnection(url, "alice", ""))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("SSL");
        }
    }

    @Test
    void require_ssl_false_allows_plain_connection() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire.Tls tls =
                new SqlGatewayProperties.PgWire.Tls(false, false, null, null, null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth,
                        null, null, null, null, tls);

        try (PgWireServer server = new PgWireServer(props, null, null, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            int port = server.getLocalPort();
            Class.forName("org.postgresql.Driver");

            String url = "jdbc:postgresql://127.0.0.1:" + port
                    + "/postgres?ssl=false&socketTimeout=5&preferQueryMode=simple";
            try (Connection c = DriverManager.getConnection(url, "alice", "")) {
                assertThat(c).isNotNull();
                assertThat(c.isClosed()).isFalse();
            }
        }
    }

    @Test
    void null_tls_config_allows_plain_connection() throws Exception {
        // No Tls record configured at all — backward-compatible default.
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth,
                        null, null, null, null, null); // tls = null

        try (PgWireServer server = new PgWireServer(props, null, null, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            int port = server.getLocalPort();
            Class.forName("org.postgresql.Driver");

            String url = "jdbc:postgresql://127.0.0.1:" + port
                    + "/postgres?ssl=false&socketTimeout=5&preferQueryMode=simple";
            try (Connection c = DriverManager.getConnection(url, "alice", "")) {
                assertThat(c.isClosed()).isFalse();
            }
        }
    }

    @Test
    void require_ssl_sends_ssl_in_error_message() throws Exception {
        // Raw TCP test: send StartupMessage directly (no SSLRequest), verify error response.
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire.Tls tls =
                new SqlGatewayProperties.PgWire.Tls(false, true, null, null, null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth,
                        null, null, null, null, tls);

        try (PgWireServer server = new PgWireServer(props, null, null, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            int port = server.getLocalPort();
            try (Socket s = new Socket("127.0.0.1", port);
                 DataOutputStream out = new DataOutputStream(s.getOutputStream());
                 DataInputStream in = new DataInputStream(s.getInputStream())) {

                // Send StartupMessage (no SSLRequest) — protocol 3.0, user=test
                byte[] userParam = "user\0test\0\0".getBytes(StandardCharsets.UTF_8);
                out.writeInt(4 + 4 + userParam.length); // length
                out.writeInt(196608);                    // protocol 3.0
                out.write(userParam);
                out.flush();

                // Expect an ErrorResponse ('E') immediately.
                byte msgType = in.readByte();
                assertThat((char) msgType).isEqualTo('E');

                int msgLen = in.readInt();
                byte[] body = in.readNBytes(msgLen - 4);
                String errorBody = new String(body, StandardCharsets.UTF_8);

                // Error body must contain SQLSTATE 28000 and mention SSL.
                assertThat(errorBody).contains("28000");
                assertThat(errorBody).containsIgnoringCase("SSL");
            }
        }
    }
}
