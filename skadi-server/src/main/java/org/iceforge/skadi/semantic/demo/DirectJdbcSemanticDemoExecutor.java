package org.iceforge.skadi.semantic.demo;

import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.iceforge.skadi.query.QueryModels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Demo-only semantic executor that runs rendered SQL directly via JDBC.
 *
 * <p>This class is intentionally narrow in scope:
 * <ul>
 *   <li>It only serves the {@code POST /api/semantic/v1/demo/query} endpoint.</li>
 *   <li>It bypasses the query cache — every request hits the database.</li>
 *   <li>It uses the server-side {@code JdbcClientFactory} and the configured
 *       {@code datasource-id}; credentials never appear in requests or responses.</li>
 *   <li>Filter values are bound via {@link PreparedStatement}, never concatenated.</li>
 * </ul>
 *
 * <p>Do not use this class outside {@code skadi-server}. It must not move into
 * {@code skadi-semantic} or {@code skadi-sql-gateway}.
 *
 * <p>Production convergence requires a future DQR/ADR covering semantic cache identity.
 */
public final class DirectJdbcSemanticDemoExecutor {

    private static final Logger log = LoggerFactory.getLogger(DirectJdbcSemanticDemoExecutor.class);

    private final JdbcClientFactory      factory;
    private final SemanticDemoSqlRenderer renderer;
    private final String                 datasourceId;
    private final int                    queryTimeoutSeconds;

    private DirectJdbcSemanticDemoExecutor(JdbcClientFactory factory,
                                           SemanticDemoSqlRenderer renderer,
                                           String datasourceId,
                                           int queryTimeoutSeconds) {
        this.factory             = Objects.requireNonNull(factory,      "factory must not be null");
        this.renderer            = Objects.requireNonNull(renderer,     "renderer must not be null");
        this.datasourceId        = Objects.requireNonNull(datasourceId, "datasourceId must not be null");
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    /**
     * @param factory             the server-side JDBC client factory; must not be null
     * @param renderer            the allowlist SQL renderer; must not be null
     * @param datasourceId        the configured datasource identifier; must not be null
     * @param queryTimeoutSeconds JDBC query timeout in seconds; zero means no timeout
     */
    public static DirectJdbcSemanticDemoExecutor create(JdbcClientFactory factory,
                                                        SemanticDemoSqlRenderer renderer,
                                                        String datasourceId,
                                                        int queryTimeoutSeconds) {
        return new DirectJdbcSemanticDemoExecutor(factory, renderer, datasourceId, queryTimeoutSeconds);
    }

    /**
     * Renders SQL from {@code request}, executes it via JDBC, and returns a tabular response.
     *
     * <p>Never throws — execution failures are captured and returned as a safe response
     * with empty rows. No credentials, tokens, or raw exception messages are included
     * in the returned {@link SemanticDemoResponse}.
     *
     * @param request the validated semantic query request; must not be null
     * @return the execution response; never null
     */
    public SemanticDemoResponse execute(SemanticDemoRequest request) {
        Objects.requireNonNull(request, "request must not be null");

        SemanticDemoSqlRenderer.RenderedSemanticDemoQuery rendered;
        try {
            rendered = renderer.render(request);
        } catch (SemanticDemoSqlRenderException ex) {
            log.warn("Semantic demo SQL render rejected [contractId={}, measure={}]: {}",
                    request.contractId(), request.measure(), ex.getMessage());
            return failureResponse(request, "SQL render rejected: " + ex.getMessage());
        }

        var jdbcRef = new QueryModels.QueryRequest.Jdbc(
                null, null, null, null, null, datasourceId, null);

        try (Connection conn = factory.openConnection(jdbcRef)) {
            try (PreparedStatement stmt = conn.prepareStatement(rendered.execSql())) {
                if (queryTimeoutSeconds > 0) {
                    stmt.setQueryTimeout(queryTimeoutSeconds);
                }
                List<Object> params = rendered.paramValues();
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    List<Map<String, Object>> rows = mapResultSet(rs);
                    log.info("Semantic demo JDBC query completed [contractId={}, datasourceId={}, rows={}]",
                            request.contractId(), datasourceId, rows.size());
                    return new SemanticDemoResponse(
                            SemanticDemoExecutionMode.JDBC_DIRECT.name(),
                            request.contractId(),
                            rendered.displaySql(),
                            rendered.columns(),
                            rows,
                            rows.size(),
                            rendered.explanation());
                }
            }
        } catch (Exception ex) {
            // Log the exception type and context only — no connection properties, no credentials.
            log.error("Semantic demo JDBC execution failed [contractId={}, datasourceId={}, error={}]",
                    request.contractId(), datasourceId, ex.getClass().getSimpleName());
            return failureResponse(request, "Query execution failed. Check server logs for details.");
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static List<Map<String, Object>> mapResultSet(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= colCount; i++) {
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    private static SemanticDemoResponse failureResponse(SemanticDemoRequest request,
                                                        String safeExplanation) {
        return new SemanticDemoResponse(
                SemanticDemoExecutionMode.JDBC_DIRECT.name(),
                request.contractId(),
                null,
                List.of(),
                List.of(),
                0,
                safeExplanation);
    }
}
