package org.iceforge.skadi.sqlgateway.mysqwire;

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

/** MySQL wire-protocol TCP server. Accepts connections and spawns {@link MySqlWireSession}s. */
public class MySqlWireServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MySqlWireServer.class);

    private final SqlGatewayProperties.MySqlWire props;
    private final SqlGatewayProperties.Metadata metadataProps;

    private final ExecutorService acceptor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "mysql-acceptor");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mysql-session");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;
    private volatile ServerSocket serverSocket;
    private volatile int localPort;

    public MySqlWireServer(SqlGatewayProperties.MySqlWire props,
                           SqlGatewayProperties.Metadata metadataProps) {
        this.props = Objects.requireNonNull(props, "props");
        this.metadataProps = metadataProps;
    }

    public void start() throws IOException {
        if (running) return;
        running = true;

        InetAddress bind = InetAddress.getByName(props.host());
        ServerSocket ss = new ServerSocket(props.port(), 50, bind);
        ss.setSoTimeout((int) Duration.ofSeconds(1).toMillis());
        this.serverSocket = ss;
        this.localPort = ss.getLocalPort();

        log.info("MySQL wire server listening on {}:{} (authMode={})",
                props.host(), this.localPort,
                props.auth() == null ? "trust" : props.auth().mode());

        acceptor.submit(() -> {
            while (running) {
                try {
                    Socket client = ss.accept();
                    sessions.submit(new MySqlWireSession(client, props, metadataProps));
                } catch (java.net.SocketTimeoutException ignored) {
                    // loop to check running flag
                } catch (IOException e) {
                    if (running) log.warn("mysql accept error: {}", e.getMessage());
                }
            }
        });
    }

    public int getLocalPort() { return localPort; }

    @Override
    public void close() {
        running = false;
        ServerSocket ss = serverSocket;
        if (ss != null) {
            try { ss.close(); } catch (IOException ignored) {}
        }
        acceptor.shutdownNow();
        sessions.shutdownNow();
    }
}
