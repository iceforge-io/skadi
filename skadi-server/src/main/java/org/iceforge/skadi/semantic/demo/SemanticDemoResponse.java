package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Outbound DTO for {@code POST /api/semantic/v1/demo/query}.
 *
 * <p>Contains the execution mode used, the generated SQL (for demo transparency),
 * the column names, result rows, row count, and a plain-English explanation.
 *
 * <p>Credentials, tokens, and passwords are never present in any field of this record.
 * On execution failure, {@link #rows()} is empty and {@link #explanation()} contains
 * a safe diagnostic category — not a raw exception message.
 *
 * <pre>{@code
 * {
 *   "executionMode": "JDBC_DIRECT",
 *   "contractId": "market-risk-demo",
 *   "generatedSql": "select legal_entity, sum(var_amount) as var_amount from ... limit 100",
 *   "columns": ["legal_entity", "var_amount"],
 *   "rows": [{"legal_entity": "LE1", "var_amount": 12345.67}],
 *   "rowCount": 1,
 *   "explanation": "Grouped var by legal_entity using the market-risk demo contract."
 * }
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticDemoResponse(
        String executionMode,
        String contractId,
        String generatedSql,
        List<String> columns,
        List<Map<String, Object>> rows,
        int rowCount,
        String explanation) {

    public SemanticDemoResponse {
        Objects.requireNonNull(executionMode, "executionMode must not be null");
        Objects.requireNonNull(contractId,    "contractId must not be null");
        Objects.requireNonNull(columns,       "columns must not be null");
        Objects.requireNonNull(rows,          "rows must not be null");
        columns = List.copyOf(columns);
        rows    = List.copyOf(rows);
    }
}
