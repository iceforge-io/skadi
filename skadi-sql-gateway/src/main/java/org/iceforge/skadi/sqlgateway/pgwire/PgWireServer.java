package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Minimal PostgreSQL wire-protocol server.
 *
 * <p>MVP scope:
 * <ul>
 *   <li>SSLRequest -> 'N' (no TLS)</li>
 *   <li>StartupMessage + (optional) cleartext password auth</li>
 *   <li>Simple Query (Q) for a few synthetic queries (e.g. SELECT 1)</li>
 *   <li>ErrorResponse + ReadyForQuery</li>
 * </ul>
 */
public class PgWireServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PgWireServer.class);

    private final SqlGatewayProperties.PgWire props;
    private final ExecutorService acceptor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "pgwire-acceptor");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "pgwire-session");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;
    private volatile ServerSocket serverSocket;
    private volatile int localPort;

    public PgWireServer(SqlGatewayProperties.PgWire props) {
        this.props = Objects.requireNonNull(props, "props");
    }

    public void start() throws IOException {
        if (running) return;
        running = true;

        InetAddress bind = InetAddress.getByName(props.host());
        ServerSocket ss = new ServerSocket(props.port(), 50, bind);
        ss.setSoTimeout((int) Duration.ofSeconds(1).toMillis());
        this.serverSocket = ss;
        this.localPort = ss.getLocalPort();

        log.info("PgWire server listening on {}:{} (authMode={})", props.host(), this.localPort,
                props.auth() == null ? "trust" : props.auth().mode());

        acceptor.submit(() -> {
            while (running) {
                try {
                    Socket s = ss.accept();
                    s.setTcpNoDelay(true);
                    s.setSoTimeout((int) Duration.ofMinutes(5).toMillis());
                    sessions.submit(new PgWireSession(s, props));
                } catch (java.net.SocketTimeoutException expected) {
                    // Normal when we set SO_TIMEOUT so we can check `running` regularly.
                } catch (IOException e) {
                    if (running) {
                        log.warn("pgwire accept failed", e);
                    }
                }
            }
        });
    }

    public int getLocalPort() {
        return localPort;
    }

    @Override
    public void close() {
        running = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
        acceptor.shutdownNow();
        sessions.shutdownNow();
    }
}
