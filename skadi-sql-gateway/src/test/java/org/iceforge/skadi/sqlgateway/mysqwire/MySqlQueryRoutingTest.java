package org.iceforge.skadi.sqlgateway.mysqwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B8 — Verifies COM_QUERY routing via raw socket against a MySQL wire server backed by H2.
 * No MySQL JDBC driver is required — packets are crafted manually.
 */
class MySqlQueryRoutingTest {

    private static MySqlWireServer SERVER;
    private static int PORT;

    @BeforeAll
    static void startServer() throws Exception {
        // Wire H2 as the backend executor
        Class.forName("org.h2.Driver");
        Connection h2 = DriverManager.getConnection("jdbc:h2:mem:mysql_routing;DB_CLOSE_DELAY=-1", "sa", "");
        h2.createStatement().execute("CREATE TABLE IF NOT EXISTS items (id INT, name VARCHAR(50))");
        h2.createStatement().execute("DELETE FROM items");
        h2.createStatement().execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')");

        MySqlExecutorProviderHolder.set(
                () -> DriverManager.getConnection("jdbc:h2:mem:mysql_routing", "sa", ""));

        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.MySqlWire props =
                new SqlGatewayProperties.MySqlWire(true, "127.0.0.1", 0, auth, null, null);

        SERVER = new MySqlWireServer(props, null);
        SERVER.start();
        for (int i = 0; i < 50 && SERVER.getLocalPort() == 0; i++) Thread.sleep(10);
        PORT = SERVER.getLocalPort();
    }

    @AfterAll
    static void stopServer() {
        if (SERVER != null) SERVER.close();
        MySqlExecutorProviderHolder.set(null);
    }

    @Test
    void com_query_select_one_returns_result_set() throws Exception {
        try (MysqlClient c = connect()) {
            byte[][] packets = c.query("SELECT 1");
            // First response packet after column-count should have column defs + rows + EOF
            // The column count packet contains a single length-encoded integer (1)
            assertThat(packets[0][0] & 0xFF).isEqualTo(1); // column count = 1
        }
    }

    @Test
    void com_query_returns_multiple_rows() throws Exception {
        try (MysqlClient c = connect()) {
            // Column count + 2 col-defs + EOF + 2 rows + EOF = 7 packets
            byte[][] packets = c.query("SELECT id, name FROM items ORDER BY id");
            // packet[0] = column count (2 columns)
            assertThat(packets[0][0] & 0xFF).isEqualTo(2);
            // Total packet count: 1 (colcnt) + 2 (coldefs) + 1 (EOF) + 2 (rows) + 1 (EOF) = 7
            assertThat(packets.length).isEqualTo(7);
        }
    }

    @Test
    void com_query_row_values_encoded_as_length_prefixed_strings() throws Exception {
        try (MysqlClient c = connect()) {
            byte[][] packets = c.query("SELECT id, name FROM items ORDER BY id");
            // First data row is packet index 4 (0=colcnt, 1=coldef, 2=coldef, 3=EOF, 4=row1)
            byte[] row1 = packets[4];
            // First column "1": length=1, data="1"
            assertThat(row1[0] & 0xFF).isEqualTo(1);  // length prefix
            assertThat((char) row1[1]).isEqualTo('1'); // value
        }
    }

    @Test
    void com_ping_returns_ok() throws Exception {
        try (MysqlClient c = connect()) {
            byte[] resp = c.ping();
            assertThat(resp[0] & 0xFF).isEqualTo(0x00); // OK
        }
    }

    @Test
    void information_schema_tables_served_from_metadata_facade() throws Exception {
        try (MysqlClient c = connect()) {
            byte[][] packets = c.query("SELECT table_name FROM information_schema.tables");
            // Should not error (0xff first byte)
            assertThat(packets[0][0] & 0xFF).isNotEqualTo(0xff);
        }
    }

    @Test
    void unsupported_command_returns_error() throws Exception {
        try (MysqlClient c = connect()) {
            // COM_STATISTICS (0x09) — not implemented
            byte[] resp = c.sendRawCommand(new byte[] { 0x09 });
            assertThat(resp[0] & 0xFF).isEqualTo(0xff); // ERR packet
        }
    }

    // ── Minimal MySQL client ──────────────────────────────────────────────────

    static class MysqlClient implements AutoCloseable {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;

        MysqlClient(int port) throws Exception {
            socket = new Socket("127.0.0.1", port);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            // Read greeting
            readPacket(); // greeting

            // Send trust handshake
            sendHandshake("test");

            // Read OK
            readPacket();
        }

        /** Sends COM_QUERY and collects all response packets until the final EOF. */
        byte[][] query(String sql) throws Exception {
            byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[1 + sqlBytes.length];
            payload[0] = 0x03; // COM_QUERY
            System.arraycopy(sqlBytes, 0, payload, 1, sqlBytes.length);
            writePacket(0, payload);

            // Collect packets: colcnt + coldefs + EOF + rows + EOF
            byte[] colCountPkt = readPacket();
            int colCount = colCountPkt[0] & 0xFF;

            java.util.List<byte[]> packets = new java.util.ArrayList<>();
            packets.add(colCountPkt);

            // Check for immediate error
            if (colCount == 0xff) return packets.toArray(new byte[0][]);
            // Check for OK (no result set)
            if (colCount == 0x00) return packets.toArray(new byte[0][]);

            // Read colCount column definition packets
            for (int i = 0; i < colCount; i++) packets.add(readPacket());
            // EOF after defs
            packets.add(readPacket());
            // Rows until EOF
            while (true) {
                byte[] pkt = readPacket();
                packets.add(pkt);
                if ((pkt[0] & 0xFF) == 0xfe && pkt.length < 9) break; // EOF
            }
            return packets.toArray(new byte[0][]);
        }

        byte[] ping() throws Exception {
            writePacket(0, new byte[] { 0x0e });
            return readPacket();
        }

        byte[] sendRawCommand(byte[] payload) throws Exception {
            writePacket(0, payload);
            return readPacket();
        }

        private void sendHandshake(String username) throws Exception {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            int caps = 0x00000200 | 0x00008000; // PROTOCOL_41 | SECURE_CONNECTION
            b.write(caps & 0xFF); b.write((caps >> 8) & 0xFF);
            b.write((caps >> 16) & 0xFF); b.write((caps >> 24) & 0xFF);
            b.write(new byte[4]); // max packet size
            b.write(0x21);        // charset
            b.write(new byte[23]); // reserved
            b.write(username.getBytes(StandardCharsets.UTF_8)); b.write(0);
            b.write(0); // auth response length = 0 (trust)
            writePacket(1, b.toByteArray());
        }

        private void writePacket(int seq, byte[] payload) throws Exception {
            out.write(payload.length & 0xFF);
            out.write((payload.length >> 8) & 0xFF);
            out.write((payload.length >> 16) & 0xFF);
            out.write(seq & 0xFF);
            out.write(payload);
            out.flush();
        }

        private byte[] readPacket() throws Exception {
            int b0 = in.read(); if (b0 < 0) throw new java.io.EOFException();
            int b1 = in.read(); int b2 = in.read();
            in.read(); // seq
            int len = b0 | (b1 << 8) | (b2 << 16);
            if (len == 0) return new byte[0];
            return in.readNBytes(len);
        }

        @Override
        public void close() throws Exception { socket.close(); }
    }

    private static MysqlClient connect() throws Exception { return new MysqlClient(PORT); }
}
