package org.iceforge.skadi.semantic.demo;

import org.iceforge.skadi.jdbc.SkadiJdbcProperties;
import org.iceforge.skadi.jdbc.spi.DefaultDriverManagerJdbcConnectionProvider;
import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link DirectJdbcSemanticDemoExecutor}.
 *
 * <p>Uses H2 in-memory databases. No Databricks, no S3, no Spring context.
 * Tests real JDBC execution via hand-rolled {@link JdbcClientFactory} instances.
 */
class DirectJdbcSemanticDemoExecutorTest {

    private static final String VIEW_A = "market_risk_exec_a";
    private static final String VIEW_B = "market_risk_exec_b";

    private static final String H2_URL_A =
            "jdbc:h2:mem:demo_exec_a;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE";
    private static final String H2_URL_B =
            "jdbc:h2:mem:demo_exec_b;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE";

    private Connection connA;
    private Connection connB;

    @BeforeEach
    void setUp() throws Exception {
        connA = DriverManager.getConnection(H2_URL_A, "sa", "");
        connB = DriverManager.getConnection(H2_URL_B, "sa", "");

        createTable(connA, VIEW_A);
        createTable(connB, VIEW_B);

        insert(connA, VIEW_A, "LE1", "CIB",  1000.0, 200.0, 50.0);
        insert(connA, VIEW_A, "LE2", "CORP", 2000.0, 400.0, 80.0);
        insert(connB, VIEW_B, "LE9", "OTHER", 9999.0, 100.0, 1.0);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connA != null) connA.close();
        if (connB != null) connB.close();
    }

    // ── 1. Uses configured datasource id ──────────────────────────────────────

    @Test
    void jdbcDirectMode_usesConfiguredDatasourceId_notHardCoded() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertEquals(2, resp.rowCount(), "should query ds-a which has 2 rows");
        assertTrue(resp.rows().stream().anyMatch(r -> "LE1".equals(r.get("legal_entity"))),
                "LE1 only exists in ds-a");
        assertTrue(resp.rows().stream().noneMatch(r -> "LE9".equals(r.get("legal_entity"))),
                "LE9 must not appear — it lives in ds-b");
    }

    @Test
    void jdbcDirectMode_differentDatasourceId_queriesDifferentData() {
        var executorB = buildExecutor("ds-b", VIEW_B);
        var resp = executorB.execute(req("var", List.of("legal_entity"), Map.of()));

        assertEquals(1, resp.rowCount(), "ds-b has exactly 1 row");
        assertEquals("LE9", resp.rows().get(0).get("legal_entity"));
    }

    // ── 2. ResultSet maps into tabular DTO ─────────────────────────────────────

    @Test
    void resultSet_mapsIntoRows() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertNotNull(resp.rows());
        assertFalse(resp.rows().isEmpty(), "rows must not be empty for a query with data");
        for (var row : resp.rows()) {
            assertTrue(row.containsKey("legal_entity"), "each row must have legal_entity");
            assertTrue(row.containsKey("var_amount"),   "each row must have var_amount (SUM)");
        }
    }

    @Test
    void resultSet_columnNamesMatchRendered() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertNotNull(resp.columns());
        assertTrue(resp.columns().contains("legal_entity"));
        assertTrue(resp.columns().contains("var_amount"));
    }

    @Test
    void resultSet_rowCountMatchesRowsSize() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertEquals(resp.rowCount(), resp.rows().size(),
                "rowCount must match rows.size()");
    }

    @Test
    void filterApplied_reducesRows() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of("lob", "CIB")));

        assertEquals(1, resp.rowCount(), "filter lob=CIB should return only CIB row");
        assertEquals("LE1", resp.rows().get(0).get("legal_entity"));
    }

    // ── 3. Unknown datasource → safe failure response ─────────────────────────

    @Test
    void unknownDatasourceId_returnsSafeFailureResponse() {
        var executor = buildExecutor("no-such-ds", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertEquals(0, resp.rowCount());
        assertTrue(resp.rows().isEmpty());
        assertNotNull(resp.explanation(), "explanation must be present even on failure");
        assertFalse(resp.explanation().isBlank());
    }

    // ── 4. No credentials in response or diagnostics ─────────────────────────

    @Test
    void response_doesNotContainPassword() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertResponseContainsNoCredentials(resp);
    }

    @Test
    void failureResponse_doesNotContainPassword() {
        var executor = buildExecutor("no-such-ds", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertResponseContainsNoCredentials(resp);
    }

    @Test
    void response_executionModeIsJdbcDirect() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertEquals("JDBC_DIRECT", resp.executionMode());
    }

    @Test
    void response_contractIdPreserved() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var resp = executor.execute(req("var", List.of("legal_entity"), Map.of()));

        assertEquals("market-risk-demo", resp.contractId());
    }

    // ── 5. SQL render failure → safe response ─────────────────────────────────

    @Test
    void invalidMeasure_renderFailure_returnsSafeResponse() {
        var executor = buildExecutor("ds-a", VIEW_A);
        var bad = new SemanticDemoRequest("q", "market-risk-demo", "delta_exposure",
                List.of("legal_entity"), Map.of());
        var resp = executor.execute(bad);

        assertEquals(0, resp.rowCount());
        assertTrue(resp.rows().isEmpty());
        assertNotNull(resp.explanation());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private DirectJdbcSemanticDemoExecutor buildExecutor(String datasourceId, String viewName) {
        SkadiJdbcProperties props = new SkadiJdbcProperties();

        SkadiJdbcProperties.JdbcDatasourceConfig cfgA = new SkadiJdbcProperties.JdbcDatasourceConfig();
        cfgA.setJdbcUrl(H2_URL_A);
        cfgA.setUsername("sa");
        cfgA.setPassword("");
        props.getDatasources().put("ds-a", cfgA);

        SkadiJdbcProperties.JdbcDatasourceConfig cfgB = new SkadiJdbcProperties.JdbcDatasourceConfig();
        cfgB.setJdbcUrl(H2_URL_B);
        cfgB.setUsername("sa");
        cfgB.setPassword("");
        props.getDatasources().put("ds-b", cfgB);

        var factory  = new JdbcClientFactory(props, List.of(new DefaultDriverManagerJdbcConnectionProvider()));
        var renderer = SemanticDemoSqlRenderer.create(viewName, 100);
        return DirectJdbcSemanticDemoExecutor.create(factory, renderer, datasourceId, 5);
    }

    private static SemanticDemoRequest req(String measure, List<String> groupBy,
                                           Map<String, String> filters) {
        return new SemanticDemoRequest("test question", "market-risk-demo", measure, groupBy, filters);
    }

    private static void createTable(Connection conn, String tableName) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("create table if not exists " + tableName + " ("
                    + "legal_entity varchar(64), "
                    + "lob varchar(64), "
                    + "desk varchar(64), "
                    + "risk_class varchar(64), "
                    + "cob_date date, "
                    + "var_amount decimal(18,4), "
                    + "es_amount decimal(18,4), "
                    + "sensitivity_amount decimal(18,4)"
                    + ")");
        }
    }

    private static void insert(Connection conn, String table, String legalEntity, String lob,
                                double varAmt, double esAmt, double sensAmt) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("insert into " + table + " values ("
                    + "'" + legalEntity + "', "
                    + "'" + lob + "', "
                    + "'RATES', "
                    + "'IR', "
                    + "current_date, "
                    + varAmt + ", "
                    + esAmt + ", "
                    + sensAmt
                    + ")");
        }
    }

    private static void assertResponseContainsNoCredentials(SemanticDemoResponse resp) {
        String[] sensitiveTerms = {"password", "token", "secret", "credential", "passwd"};
        for (String term : sensitiveTerms) {
            if (resp.explanation() != null) {
                assertFalse(resp.explanation().toLowerCase().contains(term),
                        "explanation must not contain '" + term + "': " + resp.explanation());
            }
            if (resp.generatedSql() != null) {
                assertFalse(resp.generatedSql().toLowerCase().contains(term),
                        "generatedSql must not contain '" + term + "'");
            }
        }
    }
}
