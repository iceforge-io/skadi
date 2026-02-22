package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

@Component
public class PgWireServerLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(PgWireServerLifecycle.class);

    private final SqlGatewayProperties props;
    private volatile PgWireServer server;
    private volatile boolean running;

    public PgWireServerLifecycle(SqlGatewayProperties props) {
        this.props = Objects.requireNonNull(props, "props");
    }

    @Override
    public void start() {
        SqlGatewayProperties.PgWire pg = props.pgwire();
        if (pg == null || !pg.enabled()) {
            log.info("PgWire server disabled");
            return;
        }

        try {
            PgWireServer s = new PgWireServer(pg);
            s.start();
            this.server = s;
            this.running = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start pgwire server", e);
        }
    }

    @Override
    public void stop() {
        this.running = false;
        PgWireServer s = this.server;
        if (s != null) {
            s.close();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    public PgWireServer getServer() {
        return server;
    }
}

