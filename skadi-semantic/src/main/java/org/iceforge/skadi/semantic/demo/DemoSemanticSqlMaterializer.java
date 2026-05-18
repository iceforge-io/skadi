package org.iceforge.skadi.semantic.demo;

import java.time.LocalDate;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Substitutes named bind-parameter placeholders in a {@link DemoSemanticRenderedSql}
 * with safe SQL literals, producing final SQL text suitable for
 * {@link org.iceforge.skadi.semantic.service.QueryExecutionRequest#sqlOnly}.
 *
 * <h2>Why this exists</h2>
 * <p>{@code QueryExecutionRequest.sqlOnly()} carries only SQL text; there is no
 * named-parameter carrier in the current execution seam. Rather than send unresolved
 * {@code :param} placeholders to Databricks, the demo pipeline materializes them here
 * using strict, demo-scoped rules before handing off to execution.
 *
 * <h2>Safety guarantees</h2>
 * <ul>
 *   <li><b>Only known params are substituted</b> — every {@code :word} token in the SQL
 *       must appear in {@link DemoSemanticRenderedSql#params()}; any unrecognised token
 *       throws {@link DemoSemanticSqlMaterializationException}.</li>
 *   <li><b>String injection is neutralised</b> — string values are single-quoted and all
 *       internal single-quotes are doubled ({@code '} → {@code ''}).</li>
 *   <li><b>Numeric values are rendered without quotes</b> — only {@code Integer} and
 *       {@code Long} (and other {@code Number}) values receive this treatment.</li>
 *   <li><b>Date values</b> — {@link LocalDate} renders as {@code DATE 'yyyy-MM-dd'};
 *       date strings stored as {@code String} (as produced by the renderer) render as
 *       single-quoted strings, which Databricks SQL implicitly casts to date in context.</li>
 *   <li><b>Identifiers are never substituted</b> — column names, table names, and SQL
 *       keywords in the template come only from the whitelisted renderer; this class only
 *       touches {@code :param} placeholders.</li>
 * </ul>
 *
 * <h2>Supported param value types</h2>
 * <ul>
 *   <li>{@link String} → {@code 'escaped value'} (internal {@code '} doubled)</li>
 *   <li>{@link Integer} / {@link Long} → bare numeric literal</li>
 *   <li>{@link LocalDate} → {@code DATE 'yyyy-MM-dd'}</li>
 *   <li>Other {@link Number} subtypes → bare numeric literal via {@code toString()}</li>
 * </ul>
 *
 * <pre>{@code
 * DemoSemanticRenderedSql rendered = renderer.render(interpretation);
 * String finalSql = DemoSemanticSqlMaterializer.materialize(rendered);
 * QueryExecutionRequest.sqlOnly(ctx, finalSql, CacheContract.none());
 * }</pre>
 */
public final class DemoSemanticSqlMaterializer {

    /** Matches any {@code :word} token (colon followed by one or more word characters). */
    private static final Pattern NAMED_PARAM = Pattern.compile(":(\\w+)");

    private DemoSemanticSqlMaterializer() {}

    /**
     * Materializes {@code rendered} by replacing all {@code :param_name} placeholders
     * with safe SQL literals derived from {@link DemoSemanticRenderedSql#params()}.
     *
     * @param rendered the whitelisted rendered SQL with param map; must not be null
     * @return final SQL text with all placeholders resolved; never null or blank
     * @throws DemoSemanticSqlMaterializationException if any placeholder is not in the
     *         params map, if a param value is null, or if a value type is unsupported
     */
    public static String materialize(DemoSemanticRenderedSql rendered) {
        String              sql    = rendered.sql();
        Map<String, Object> params = rendered.params();

        Matcher      matcher = NAMED_PARAM.matcher(sql);
        StringBuffer result  = new StringBuffer();

        while (matcher.find()) {
            String paramName = matcher.group(1);

            if (!params.containsKey(paramName)) {
                throw new DemoSemanticSqlMaterializationException(
                        "No parameter value found for placeholder: :" + paramName);
            }

            Object value   = params.get(paramName);
            String literal = toLiteral(paramName, value);
            matcher.appendReplacement(result, Matcher.quoteReplacement(literal));
        }

        matcher.appendTail(result);
        return result.toString();
    }

    // ── Type-to-literal conversion ─────────────────────────────────────────────

    private static String toLiteral(String paramName, Object value) {
        if (value == null) {
            throw new DemoSemanticSqlMaterializationException(
                    "Parameter value is null for: " + paramName);
        }
        if (value instanceof String s) {
            // Single-quote the value; double any internal single quotes to neutralise injection.
            return "'" + s.replace("'", "''") + "'";
        }
        if (value instanceof Integer || value instanceof Long) {
            return value.toString();
        }
        if (value instanceof LocalDate d) {
            return "DATE '" + d + "'";
        }
        if (value instanceof Number n) {
            return n.toString();
        }
        throw new DemoSemanticSqlMaterializationException(
                "Unsupported parameter value type " + value.getClass().getSimpleName()
                + " for parameter: " + paramName);
    }
}
