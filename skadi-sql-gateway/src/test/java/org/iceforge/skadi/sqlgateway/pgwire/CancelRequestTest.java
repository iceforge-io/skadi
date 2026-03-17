package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a CancelRequest sent on a separate TCP connection reaches the target session
 * and does not crash the server.
 */
class CancelRequestTest {

    @Test
    void cancelRequestIsHandledWithoutCrash() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth, null, null, null, null);

        try (PgWireServer server = new PgWireServer(props, null, null, null)) {
            server.start();
            for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);

            int port = server.getLocalPort();

            // Connect a regular session so we have a valid pid to cancel.
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://127.0.0.1:" + port
                    + "/postgres?ssl=false&socketTimeout=5&preferQueryMode=simple";
            try (Connection c = DriverManager.getConnection(url, "user", "");
                 Statement st = c.createStatement()) {

                // Send a CancelRequest for a fake (non-existent) pid — server should not crash.
                sendCancelRequest("127.0.0.1", port, 9999, 9999);

                // Normal query must still work after the cancel.
                var rs = st.executeQuery("select 1");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }
    }

    private static void sendCancelRequest(String host, int port, int pid, int secretKey) throws Exception {
        try (Socket s = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(s.getOutputStream())) {
            out.writeInt(16);           // length
            out.writeInt(80877102);     // cancel request code
            out.writeInt(pid);
            out.writeInt(secretKey);
            out.flush();
        }
    }
}
