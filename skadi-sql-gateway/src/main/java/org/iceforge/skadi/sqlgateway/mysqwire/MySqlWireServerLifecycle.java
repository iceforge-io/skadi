package org.iceforge.skadi.sqlgateway.mysqwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

@Component
public class MySqlWireServerLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(MySqlWireServerLifecycle.class);

    private final SqlGatewayProperties props;
    private volatile MySqlWireServer server;
    private volatile boolean running;

    public MySqlWireServerLifecycle(SqlGatewayProperties props) {
        this.props = Objects.requireNonNull(props, "props");
    }

    @Override
    public void start() {
        SqlGatewayProperties.MySqlWire mysql = props.mysqlwire();
        if (mysql == null || !mysql.enabled()) {
            log.info("MySQL wire server disabled");
            return;
        }

        try {
            MySqlWireServer s = new MySqlWireServer(mysql, props.metadata());
            s.start();
            this.server = s;
            this.running = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start MySQL wire server", e);
        }
    }

    @Override
    public void stop() {
        this.running = false;
        MySqlWireServer s = this.server;
        if (s != null) s.close();
    }

    @Override
    public boolean isRunning() { return running; }

    @Override
    public int getPhase() { return 0; }

    public MySqlWireServer getServer() { return server; }
}
