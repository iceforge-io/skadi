package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * POC coverage for Story A5: information_schema facade.
 *
 * <p>We assert that metadata discovery queries return a non-empty result set.
 */
public class InformationSchemaFacadeTest {

    @Test
    void jdbcClientCanQueryInformationSchemaTables() throws Exception {
        try (PgWireServerHarness h = PgWireServerHarness.startTrust()) {
            String url = "jdbc:postgresql://127.0.0.1:" + h.port() + "/postgres?ssl=false&preferQueryMode=simple";
            try (Connection c = DriverManager.getConnection(url, "user", "")) {
                try (Statement st = c.createStatement()) {
                    try (ResultSet rs = st.executeQuery("select table_schema, table_name from information_schema.tables")) {
                        assertThat(rs.next()).isTrue();
                        assertThat(rs.getString(1)).isNotBlank();
                        assertThat(rs.getString(2)).isNotBlank();
                    }
                }
            }
        }
    }

    /** Minimal server harness shared with pgwire tests. */
    static final class PgWireServerHarness implements AutoCloseable {
        private final PgWireServer server;

        private PgWireServerHarness(PgWireServer server) {
            this.server = server;
        }

        static PgWireServerHarness startTrust() throws Exception {
            org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties.PgWire.Auth auth =
                    new org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties.PgWire.Auth("trust", java.util.Map.of());

            org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties.PgWire props =
                    new org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth, null, null);
            PgWireServer s = new PgWireServer(props);
            s.start();

            // tiny wait for server to bind
            for (int i = 0; i < 50 && s.getLocalPort() == 0; i++) {
                Thread.sleep(10);
            }
            return new PgWireServerHarness(s);
        }

        int port() {
            return server.getLocalPort();
        }

        @Override
        public void close() {
            server.close();
        }
    }
}
