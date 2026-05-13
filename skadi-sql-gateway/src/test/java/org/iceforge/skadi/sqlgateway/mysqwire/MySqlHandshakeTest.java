package org.iceforge.skadi.sqlgateway.mysqwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B8 — Verifies the MySQL wire server greeting (Handshake v10) packet.
 * Uses a raw socket so no MySQL JDBC driver is required in the test classpath.
 */
class MySqlHandshakeTest {

    private static SqlGatewayProperties.MySqlWire trustProps() {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        return new SqlGatewayProperties.MySqlWire(true, "127.0.0.1", 0, auth, null, null);
    }

    @Test
    void server_sends_handshake_v10() throws Exception {
        try (MySqlWireServer server = new MySqlWireServer(trustProps(), null)) {
            server.start();
            waitForPort(server);

            try (Socket sock = new Socket("127.0.0.1", server.getLocalPort());
                 DataInputStream in = new DataInputStream(sock.getInputStream())) {

                // Read greeting packet
                byte[] greeting = readPacket(in);

                // Protocol version must be 10
                assertThat(greeting[0] & 0xFF).isEqualTo(10);

                // Server version string ends with \0; find it
                int versionEnd = 1;
                while (versionEnd < greeting.length && greeting[versionEnd] != 0) versionEnd++;
                String version = new String(greeting, 1, versionEnd - 1, StandardCharsets.UTF_8);
                assertThat(version).startsWith("8.0");

                // Auth plugin name appears at the end; find "mysql_native_password"
                String greetingStr = new String(greeting, StandardCharsets.UTF_8);
                assertThat(greetingStr).contains("mysql_native_password");
            }
        }
    }

    @Test
    void server_sends_unique_connection_ids() throws Exception {
        try (MySqlWireServer server = new MySqlWireServer(trustProps(), null)) {
            server.start();
            waitForPort(server);

            int id1 = connectAndReadConnectionId(server.getLocalPort());
            int id2 = connectAndReadConnectionId(server.getLocalPort());
            assertThat(id1).isNotEqualTo(id2);
        }
    }

    @Test
    void trust_auth_accepted_with_empty_password() throws Exception {
        try (MySqlWireServer server = new MySqlWireServer(trustProps(), null)) {
            server.start();
            waitForPort(server);

            try (Socket sock = new Socket("127.0.0.1", server.getLocalPort());
                 DataInputStream in = new DataInputStream(sock.getInputStream());
                 DataOutputStream out = new DataOutputStream(sock.getOutputStream())) {

                readPacket(in); // greeting

                // Send minimal client handshake response (CLIENT_PROTOCOL_41, username="test", no auth data)
                sendMinimalHandshake(out, "test", new byte[0]);

                // Should receive OK packet (0x00)
                byte[] response = readPacket(in);
                assertThat(response[0] & 0xFF).as("expected OK packet (0x00)").isEqualTo(0);
            }
        }
    }

    @Test
    void password_auth_rejects_wrong_password() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("password", "plaintext",
                        Map.of("alice", "correct-password"), null);
        SqlGatewayProperties.MySqlWire props =
                new SqlGatewayProperties.MySqlWire(true, "127.0.0.1", 0, auth, null, null);

        try (MySqlWireServer server = new MySqlWireServer(props, null)) {
            server.start();
            waitForPort(server);

            try (Socket sock = new Socket("127.0.0.1", server.getLocalPort());
                 DataInputStream in = new DataInputStream(sock.getInputStream());
                 DataOutputStream out = new DataOutputStream(sock.getOutputStream())) {

                readPacket(in); // greeting

                // Send wrong password (empty token — 20 zero bytes would normally be a real hash)
                sendMinimalHandshake(out, "alice", new byte[20]);

                // Should receive ERR packet (0xff)
                byte[] response = readPacket(in);
                assertThat(response[0] & 0xFF).as("expected ERR packet (0xff)").isEqualTo(0xff);
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static int connectAndReadConnectionId(int port) throws Exception {
        try (Socket sock = new Socket("127.0.0.1", port);
             DataInputStream in = new DataInputStream(sock.getInputStream())) {
            byte[] greeting = readPacket(in);
            // Connection ID is at bytes 5..8 (after protocol version + null-terminated version)
            int versionEnd = 1;
            while (versionEnd < greeting.length && greeting[versionEnd] != 0) versionEnd++;
            int offset = versionEnd + 1;
            return (greeting[offset] & 0xFF)
                    | ((greeting[offset + 1] & 0xFF) << 8)
                    | ((greeting[offset + 2] & 0xFF) << 16)
                    | ((greeting[offset + 3] & 0xFF) << 24);
        }
    }

    private static void sendMinimalHandshake(DataOutputStream out, String username, byte[] authResponse)
            throws Exception {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        // Capability flags (4 bytes LE): CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
        int caps = 0x00000200 | 0x00008000;
        b.write(caps & 0xFF); b.write((caps >> 8) & 0xFF); b.write((caps >> 16) & 0xFF); b.write((caps >> 24) & 0xFF);
        // Max packet size
        b.write(0xff); b.write(0xff); b.write(0xff); b.write(0x00);
        // Charset
        b.write(0x21);
        // Reserved (23 zeros)
        b.write(new byte[23]);
        // Username (null-terminated)
        b.write(username.getBytes(StandardCharsets.UTF_8));
        b.write(0);
        // Auth response length + bytes
        b.write(authResponse.length & 0xFF);
        b.write(authResponse);

        byte[] payload = b.toByteArray();
        // Write packet with seq=1
        out.write(payload.length & 0xFF);
        out.write((payload.length >> 8) & 0xFF);
        out.write((payload.length >> 16) & 0xFF);
        out.write(1); // seq
        out.write(payload);
        out.flush();
    }

    static byte[] readPacket(DataInputStream in) throws Exception {
        int b0 = in.read(); if (b0 < 0) throw new java.io.EOFException();
        int b1 = in.read();
        int b2 = in.read();
        in.read(); // seq
        int len = b0 | (b1 << 8) | (b2 << 16);
        if (len == 0) return new byte[0];
        return in.readNBytes(len);
    }

    private static void waitForPort(MySqlWireServer server) throws Exception {
        for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);
    }
}
