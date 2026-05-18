package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The output of {@link DemoSemanticSqlRenderer}: a whitelisted SQL string with
 * named bind parameters and rendering metadata.
 *
 * <p>The SQL text uses {@code :param_name} placeholders for all variable values.
 * No user-provided literal values appear in {@link #sql()} — they are held
 * separately in {@link #params()} and must be bound via the executing framework's
 * named-parameter mechanism before the statement is sent to the database.
 *
 * <p>This separation is the primary injection-safety guarantee of the renderer.
 * Callers must never concatenate {@link #params()} values directly into SQL text.
 *
 * <pre>{@code
 * // Safe usage — params are bound by the JDBC framework, not concatenated:
 * DemoSemanticRenderedSql rendered = renderer.render(interpretation);
 * namedJdbcTemplate.queryForList(rendered.sql(), rendered.params());
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticRenderedSql(
        String sql,
        Map<String, Object> params,
        String contractId,
        DemoSemanticOutputShape outputShape,
        String measure,
        List<String> groupBy,
        String viewName) {

    /**
     * @param sql         SQL text with {@code :param_name} bind placeholders; must not be null or blank
     * @param params      named bind parameters; must not be null; copied defensively (unmodifiable)
     * @param contractId  the contract that was rendered; must not be null
     * @param outputShape the output shape (GRID or TIMESERIES); must not be null
     * @param measure     the selected measure column name; must not be null
     * @param groupBy     the ordered group-by column names; must not be null; copied defensively
     * @param viewName    the fully-qualified view name used in FROM; must not be null
     */
    public DemoSemanticRenderedSql {
        Objects.requireNonNull(sql,         "sql must not be null");
        Objects.requireNonNull(params,      "params must not be null");
        Objects.requireNonNull(contractId,  "contractId must not be null");
        Objects.requireNonNull(outputShape, "outputShape must not be null");
        Objects.requireNonNull(measure,     "measure must not be null");
        Objects.requireNonNull(groupBy,     "groupBy must not be null");
        Objects.requireNonNull(viewName,    "viewName must not be null");
        if (sql.isBlank()) throw new IllegalArgumentException("sql must not be blank");
        params  = Map.copyOf(params);
        groupBy = List.copyOf(groupBy);
    }
}
