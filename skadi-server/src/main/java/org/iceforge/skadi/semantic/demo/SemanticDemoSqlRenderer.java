package org.iceforge.skadi.semantic.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Allowlist-only SQL renderer for the structured semantic JDBC demo endpoint.
 *
 * <h2>Safety guarantees</h2>
 * <ul>
 *   <li><b>Allowlists</b> — contract, measure, dimension, and filter-key identifiers
 *       are each validated against an explicit set before any SQL is assembled.</li>
 *   <li><b>Parameterised filter values</b> — all filter values become {@code ?}
 *       placeholders in {@link RenderedSemanticDemoQuery#execSql()} and are held
 *       separately in {@link RenderedSemanticDemoQuery#paramValues()}.
 *       No user-supplied literal is concatenated into the SQL text.</li>
 *   <li><b>Operator-controlled view name</b> — the FROM clause uses a view name
 *       from server config, not from the request.</li>
 *   <li><b>Hard limit</b> — {@code LIMIT} is a numeric literal derived from
 *       the operator-configured {@code max-rows} value.</li>
 *   <li><b>No execution</b> — this class assembles strings only.</li>
 * </ul>
 *
 * <h2>Measure-to-column mapping</h2>
 * <table>
 *   <tr><th>Semantic measure</th><th>Physical column</th></tr>
 *   <tr><td>var</td><td>var_amount</td></tr>
 *   <tr><td>expected_shortfall</td><td>es_amount</td></tr>
 *   <tr><td>sensitivity</td><td>sensitivity_amount</td></tr>
 * </table>
 */
public final class SemanticDemoSqlRenderer {

    // ── Allowlists ─────────────────────────────────────────────────────────────

    private static final Set<String> ALLOWED_CONTRACTS = Set.of(
            "market-risk-demo");

    /** Maps semantic measure name → physical aggregate column name. */
    private static final Map<String, String> MEASURE_TO_COLUMN = Map.of(
            "var",                "var_amount",
            "expected_shortfall", "es_amount",
            "sensitivity",        "sensitivity_amount");

    private static final Set<String> ALLOWED_DIMENSIONS = Set.of(
            "legal_entity", "lob", "desk", "risk_class", "cob_date");

    private static final Set<String> ALLOWED_FILTER_KEYS = Set.of(
            "legal_entity", "lob", "desk", "risk_class", "cob_date");

    // ── Instance fields ────────────────────────────────────────────────────────

    private final String viewName;
    private final int    maxRows;

    // ── Factory ────────────────────────────────────────────────────────────────

    private SemanticDemoSqlRenderer(String viewName, int maxRows) {
        this.viewName = viewName;
        this.maxRows  = maxRows;
    }

    /**
     * @param viewName fully-qualified operator-controlled view name; must not be null or blank
     * @param maxRows  hard row cap appended as LIMIT; must be positive
     */
    public static SemanticDemoSqlRenderer create(String viewName, int maxRows) {
        Objects.requireNonNull(viewName, "viewName must not be null");
        if (viewName.isBlank()) throw new IllegalArgumentException("viewName must not be blank");
        if (maxRows <= 0)       throw new IllegalArgumentException("maxRows must be positive, was: " + maxRows);
        return new SemanticDemoSqlRenderer(viewName, maxRows);
    }

    // ── Main render method ─────────────────────────────────────────────────────

    /**
     * Renders {@code request} into a validated, parameterised SQL query.
     *
     * @param request the structured semantic request; must not be null
     * @return a rendered query with executable SQL, param values, and display SQL
     * @throws SemanticDemoSqlRenderException if any identifier fails the allowlist check
     */
    public RenderedSemanticDemoQuery render(SemanticDemoRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        validate(request);

        String measureCol = MEASURE_TO_COLUMN.get(request.measure());

        List<String> filterKeys   = new ArrayList<>(request.filters().keySet());
        List<Object> paramValues  = new ArrayList<>();
        for (String key : filterKeys) {
            paramValues.add(request.filters().get(key));
        }

        String selectCols  = buildSelectCols(request.groupBy(), measureCol, request.measure());
        String groupByCols = String.join(", ", request.groupBy());
        String whereSql    = buildWhereExec(filterKeys);

        StringBuilder exec = new StringBuilder();
        exec.append("select ").append(selectCols)
            .append("\nfrom ").append(viewName);
        if (!whereSql.isEmpty()) {
            exec.append("\nwhere ").append(whereSql);
        }
        exec.append("\ngroup by ").append(groupByCols)
            .append("\norder by ").append(groupByCols)
            .append("\nlimit ").append(maxRows);

        String displaySql  = buildDisplaySql(exec.toString(), filterKeys, paramValues);
        List<String> columns = buildColumns(request.groupBy(), measureCol);
        String explanation   = buildExplanation(request);

        return new RenderedSemanticDemoQuery(
                exec.toString(), List.copyOf(paramValues), displaySql, columns, explanation);
    }

    // ── Validation ─────────────────────────────────────────────────────────────

    private static void validate(SemanticDemoRequest req) {
        if (!ALLOWED_CONTRACTS.contains(req.contractId())) {
            throw new SemanticDemoSqlRenderException(
                    "contract not on allowlist: '" + req.contractId() + "'");
        }
        if (!MEASURE_TO_COLUMN.containsKey(req.measure())) {
            throw new SemanticDemoSqlRenderException(
                    "measure not on allowlist: '" + req.measure() + "'");
        }
        for (String dim : req.groupBy()) {
            if (!ALLOWED_DIMENSIONS.contains(dim)) {
                throw new SemanticDemoSqlRenderException(
                        "groupBy dimension not on allowlist: '" + dim + "'");
            }
        }
        for (String key : req.filters().keySet()) {
            if (!ALLOWED_FILTER_KEYS.contains(key)) {
                throw new SemanticDemoSqlRenderException(
                        "filter key not on allowlist: '" + key + "'");
            }
        }
    }

    // ── SQL building ───────────────────────────────────────────────────────────

    private static String buildSelectCols(List<String> groupBy, String measureCol, String measureAlias) {
        StringJoiner sj = new StringJoiner(", ");
        for (String dim : groupBy) {
            sj.add(dim);
        }
        sj.add("sum(" + measureCol + ") as " + measureCol);
        return sj.toString();
    }

    private static String buildWhereExec(List<String> filterKeys) {
        if (filterKeys.isEmpty()) return "";
        StringJoiner sj = new StringJoiner("\n  and ");
        for (String key : filterKeys) {
            sj.add(key + " = ?");
        }
        return sj.toString();
    }

    /** Substitutes {@code ?} placeholders with single-quoted string literals for display. */
    private static String buildDisplaySql(String execSql, List<String> keys, List<Object> values) {
        String display = execSql;
        for (Object value : values) {
            String literal = "'" + String.valueOf(value).replace("'", "''") + "'";
            display = display.replaceFirst("\\?", literal);
        }
        return display;
    }

    private static List<String> buildColumns(List<String> groupBy, String measureCol) {
        List<String> cols = new ArrayList<>(groupBy);
        cols.add(measureCol);
        return List.copyOf(cols);
    }

    private static String buildExplanation(SemanticDemoRequest req) {
        String groupByStr = String.join(", ", req.groupBy());
        return "Grouped " + req.measure() + " by " + groupByStr
                + " using the " + req.contractId() + " contract.";
    }

    // ── Result record ──────────────────────────────────────────────────────────

    /**
     * Output of {@link SemanticDemoSqlRenderer#render}.
     *
     * @param execSql     SQL with {@code ?} placeholders — use with PreparedStatement
     * @param paramValues ordered filter values to bind to {@code ?} in {@code execSql}
     * @param displaySql  materialized SQL with quoted literals — for the response {@code generatedSql} field
     * @param columns     result column names in SELECT order
     * @param explanation plain-English description of the query
     */
    public record RenderedSemanticDemoQuery(
            String       execSql,
            List<Object> paramValues,
            String       displaySql,
            List<String> columns,
            String       explanation) {

        public RenderedSemanticDemoQuery {
            Objects.requireNonNull(execSql,     "execSql must not be null");
            Objects.requireNonNull(paramValues, "paramValues must not be null");
            Objects.requireNonNull(displaySql,  "displaySql must not be null");
            Objects.requireNonNull(columns,     "columns must not be null");
            paramValues = List.copyOf(paramValues);
            columns     = List.copyOf(columns);
        }
    }
}
