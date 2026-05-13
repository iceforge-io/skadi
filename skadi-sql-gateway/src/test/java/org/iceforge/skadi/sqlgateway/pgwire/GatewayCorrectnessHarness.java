package org.iceforge.skadi.sqlgateway.pgwire;

import org.h2.jdbcx.JdbcDataSource;
import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

/**
 * Shared test harness for B4 correctness tests.
 *
 * <p>Starts a PgWireServer backed by an in-memory H2 database seeded with representative
 * data for every JDBC type the gateway must handle. Tests connect to the gateway via the
 * standard PostgreSQL JDBC driver in simple-query mode to avoid L14 (duplicate
 * RowDescription) and exercise path 1 (JDBC text rows).
 *
 * <p>Arrow path (path 3) is covered separately in {@link ValueEncodingArrowTest}.
 *
 * <p>Usage: create in @BeforeEach, close in @AfterEach.
 */
final class GatewayCorrectnessHarness implements AutoCloseable {

    static final String H2_URL = "jdbc:h2:mem:skadi_correctness;DB_CLOSE_DELAY=-1";

    static {
        seedH2();
    }

    private final PgWireServer server;

    GatewayCorrectnessHarness() throws Exception {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL(H2_URL);

        // Inject H2 as the executor; PgWireSession picks this up via SqlExecutorProviderHolder.
        SqlExecutorProviderHolder.set(h2::getConnection);

        SqlGatewayProperties.PgWire.Auth auth =
                new SqlGatewayProperties.PgWire.Auth("trust", null, Map.of(), null);
        // cacheProps = null → caching disabled; tests always hit H2.
        SqlGatewayProperties.PgWire props =
                new SqlGatewayProperties.PgWire(true, "127.0.0.1", 0, auth, null, null, null, null, null);

        this.server = new PgWireServer(props, null, null, null);
        server.start();
        for (int i = 0; i < 50 && server.getLocalPort() == 0; i++) Thread.sleep(10);
    }

    /** Returns a JDBC connection to the gateway in simple-query mode. */
    Connection jdbcClient() throws Exception {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://127.0.0.1:" + server.getLocalPort()
                + "/postgres?ssl=false&preferQueryMode=simple&socketTimeout=10";
        return DriverManager.getConnection(url, "testuser", "");
    }

    @Override
    public void close() {
        SqlExecutorProviderHolder.set(null);
        server.close();
    }

    // --- Seed tables ---

    private static void seedH2() {
        try {
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL(H2_URL);
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {

                // Main type-coverage table.
                st.execute("DROP TABLE IF EXISTS skadi_types");
                st.execute("""
                    CREATE TABLE skadi_types (
                        id_col      BIGINT,
                        int_col     INTEGER,
                        small_col   SMALLINT,
                        double_col  DOUBLE PRECISION,
                        dec_col     DECIMAL(10,2),
                        bool_col    BOOLEAN,
                        str_col     VARCHAR(100),
                        date_col    DATE,
                        ts_col      TIMESTAMP,
                        null_col    VARCHAR(10)
                    )
                """);
                st.execute("""
                    INSERT INTO skadi_types VALUES (
                        9000000000001,
                        42,
                        7,
                        3.14159,
                        1234.56,
                        TRUE,
                        'hello world',
                        '2021-06-15',
                        '2021-06-15 12:30:00',
                        NULL
                    )
                """);

                // Additional decimal edge cases.
                st.execute("DROP TABLE IF EXISTS skadi_decimals");
                st.execute("CREATE TABLE skadi_decimals (label VARCHAR(20), val DECIMAL(12,4))");
                st.execute("""
                    INSERT INTO skadi_decimals VALUES
                        ('zero',      0.0000),
                        ('positive',  1234.5600),
                        ('negative', -9.7500),
                        ('small',     0.0001)
                """);

                // Ordering test table.
                st.execute("DROP TABLE IF EXISTS skadi_orders");
                st.execute("CREATE TABLE skadi_orders (n INTEGER, label VARCHAR(10))");
                st.execute("""
                    INSERT INTO skadi_orders VALUES
                        (3,'c'),(1,'a'),(2,'b'),(4,'d'),(5,'e')
                """);

                // Null-heavy table.
                st.execute("DROP TABLE IF EXISTS skadi_nulls");
                st.execute("CREATE TABLE skadi_nulls (id INTEGER, val VARCHAR(20), num DECIMAL(8,2))");
                st.execute("""
                    INSERT INTO skadi_nulls VALUES
                        (1, NULL, NULL),
                        (2, 'present', 1.23),
                        (3, NULL, 4.56)
                """);
            }
        } catch (Exception e) {
            throw new RuntimeException("H2 seed failed", e);
        }
    }
}
