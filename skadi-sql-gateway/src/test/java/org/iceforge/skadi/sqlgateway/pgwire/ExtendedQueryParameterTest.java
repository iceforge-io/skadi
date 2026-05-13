package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that Bind ('B') message parameter values are parsed correctly and the session
 * does not crash when an extended-query flow contains parameters.
 *
 * <p>No Databricks backend is required — tests verify protocol-level behaviour only.
 */
class ExtendedQueryParameterTest {

    @Test
    void bindWithTextParamsDoesNotCrashSession() throws Exception {
        try (PgWireServer server = startTrustServer()) {
            int port = server.getLocalPort();

            try (Socket s = new Socket("127.0.0.1", port);
                 DataOutputStream out = new DataOutputStream(s.getOutputStream());
                 DataInputStream in = new DataInputStream(s.getInputStream())) {

                sendStartup(out, "testuser");
                List<PgMessage> startup = readUntil(in, 'Z'); // ReadyForQuery
                assertThat(startup).extracting(PgMessage::type).contains('Z');

                // Extended-query flow: Parse → Bind (2 params) → Describe → Execute → Sync
                String sql = "SELECT $1, $2";
                sendParse(out, "", sql, new int[0]);
                sendBind(out, "", "", new String[]{"hello", "42"});
                sendDescribePortal(out, "");
                sendExecute(out, "", 0);
                sendSync(out);
                out.flush();

                // Collect all messages until ReadyForQuery.
                List<PgMessage> resp = readUntil(in, 'Z');

                List<Character> types = resp.stream().map(PgMessage::type).toList();

                // Must receive ParseComplete (1) and BindComplete (2).
                assertThat(types).contains('1', '2');

                // Must receive ReadyForQuery — session stayed alive.
                assertThat(types).contains('Z');

                // Must NOT contain an unexpected binary garbage (protocol corruption check).
                for (PgMessage m : resp) {
                    // Valid pgwire types in this exchange.
                    assertThat("12nNTCEIZt")
                            .as("unexpected message type '%c'", m.type())
                            .containsAnyOf(String.valueOf(m.type()));
                }
            }
        }
    }

    @Test
    void bindWithNullParamDoesNotCrashSession() throws Exception {
        try (PgWireServer server = startTrustServer()) {
            int port = server.getLocalPort();

            try (Socket s = new Socket("127.0.0.1", port);
                 DataOutputStream out = new DataOutputStream(s.getOutputStream());
                 DataInputStream in = new DataInputStream(s.getInputStream())) {

                sendStartup(out, "testuser");
                readUntil(in, 'Z');

                // Bind with one SQL-NULL parameter (length = -1).
                sendParse(out, "", "SELECT $1", new int[0]);
                sendBindWithNull(out, "", "");
                sendExecute(out, "", 0);
                sendSync(out);
                out.flush();

                List<PgMessage> resp = readUntil(in, 'Z');
                List<Character> types = resp.stream().map(PgMessage::type).toList();

                assertThat(types).contains('1', '2', 'Z');
            }
        }
    }

    @Test
    void emptyBindDoesNotCrashSession() throws Exception {
        try (PgWireServer server = startTrustServer()) {
            int port = server.getLocalPort();

            try (Socket s = new Socket("127.0.0.1", port);
                 DataOutputStream out = new DataOutputStream(s.getOutputStream());
                 DataInputStream in = new DataInputStream(s.getInputStream())) {

                sendStartup(out, "testuser");
                readUntil(in, 'Z');

                // Bind with zero parameters (normal for non-parameterised prepared statements).
                sendParse(out, "", "select 1", new int[0]);
                sendBind(out, "", "", new String[0]);
                sendExecute(out, "", 0);
                sendSync(out);
                out.flush();

                List<PgMessage> resp = readUntil(in, 'Z');
                List<Character> types = resp.stream().map(PgMessage::type).toList();

                assertThat(types).contains('1', '2');
                assertThat(types).contains('C'); // CommandComplete for SELECT 1
                assertThat(types).contains('Z'); // ReadyForQuery — session stayed alive
            }
        }
    }

    // --- Protocol helpers ---

    private static PgWireServer startTrustServer() throws Exception {
        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth, null, null, null, null, null);
        PgWireServer server = new PgWireServer(props, null, null, null);
        server.start();
        for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);
        return server;
    }

    /** Reads pgwire messages from the stream until a message of the given type is seen. */
    private static List<PgMessage> readUntil(DataInputStream in, char sentinel) throws Exception {
        List<PgMessage> msgs = new ArrayList<>();
        for (int attempt = 0; attempt < 50; attempt++) {
            byte type = in.readByte();
            int len = in.readInt();
            byte[] body = in.readNBytes(len - 4);
            msgs.add(new PgMessage((char) type, body));
            if (type == sentinel) break;
        }
        return msgs;
    }

    private static void sendStartup(DataOutputStream out, String user) throws Exception {
        byte[] userKv = ("user\0" + user + "\0\0").getBytes(StandardCharsets.UTF_8);
        out.writeInt(4 + 4 + userKv.length); // total len
        out.writeInt(196608);                 // protocol 3.0
        out.write(userKv);
        out.flush();
    }

    private static void sendParse(DataOutputStream out, String stmtName, String sql, int[] paramOids) throws Exception {
        byte[] name = (stmtName + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] query = (sql + "\0").getBytes(StandardCharsets.UTF_8);
        int oidBytes = 2 + paramOids.length * 4;
        out.writeByte('P');
        out.writeInt(4 + name.length + query.length + oidBytes);
        out.write(name);
        out.write(query);
        out.writeShort(paramOids.length);
        for (int oid : paramOids) out.writeInt(oid);
    }

    private static void sendBind(DataOutputStream out, String portal, String stmt, String[] params) throws Exception {
        byte[] portalBytes = (portal + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] stmtBytes = (stmt + "\0").getBytes(StandardCharsets.UTF_8);

        // Build param bytes.
        List<byte[]> encodedParams = new ArrayList<>();
        for (String p : params) encodedParams.add(p.getBytes(StandardCharsets.UTF_8));

        int paramDataLen = 0;
        for (byte[] pb : encodedParams) paramDataLen += 4 + pb.length;

        int payloadLen = portalBytes.length + stmtBytes.length
                + 2 /* num format codes */ + 2 /* num params */ + paramDataLen + 2 /* num result formats */;
        out.writeByte('B');
        out.writeInt(4 + payloadLen);
        out.write(portalBytes);
        out.write(stmtBytes);
        out.writeShort(0); // 0 format codes → all text
        out.writeShort(params.length);
        for (byte[] pb : encodedParams) {
            out.writeInt(pb.length);
            out.write(pb);
        }
        out.writeShort(0); // 0 result format codes → all text
    }

    /** Bind with a single SQL-NULL parameter. */
    private static void sendBindWithNull(DataOutputStream out, String portal, String stmt) throws Exception {
        byte[] portalBytes = (portal + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] stmtBytes = (stmt + "\0").getBytes(StandardCharsets.UTF_8);
        int payloadLen = portalBytes.length + stmtBytes.length + 2 + 2 + 4 + 2;
        out.writeByte('B');
        out.writeInt(4 + payloadLen);
        out.write(portalBytes);
        out.write(stmtBytes);
        out.writeShort(0); // no format codes
        out.writeShort(1); // 1 param
        out.writeInt(-1);  // NULL
        out.writeShort(0); // no result format codes
    }

    private static void sendDescribePortal(DataOutputStream out, String portal) throws Exception {
        byte[] name = (portal + "\0").getBytes(StandardCharsets.UTF_8);
        out.writeByte('D');
        out.writeInt(4 + 1 + name.length);
        out.writeByte('P');
        out.write(name);
    }

    private static void sendExecute(DataOutputStream out, String portal, int maxRows) throws Exception {
        byte[] name = (portal + "\0").getBytes(StandardCharsets.UTF_8);
        out.writeByte('E');
        out.writeInt(4 + name.length + 4);
        out.write(name);
        out.writeInt(maxRows);
    }

    private static void sendSync(DataOutputStream out) throws Exception {
        out.writeByte('S');
        out.writeInt(4);
    }

    record PgMessage(char type, byte[] body) {}
}
