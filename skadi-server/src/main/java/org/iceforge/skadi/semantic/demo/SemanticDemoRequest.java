package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Inbound DTO for {@code POST /api/semantic/v1/demo/query}.
 *
 * <p>The caller provides a structured query — semantic identifiers for measure,
 * groupBy, and filters — rather than free-form SQL. All identifiers are validated
 * against explicit allowlists in {@link SemanticDemoSqlRenderer} before any SQL
 * is generated.
 *
 * <pre>{@code
 * {
 *   "question": "Show me VaR by legal entity for CIB",
 *   "contractId": "market-risk-demo",
 *   "measure": "var",
 *   "groupBy": ["legal_entity"],
 *   "filters": {"lob": "CIB"}
 * }
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticDemoRequest(
        String question,
        String contractId,
        String measure,
        List<String> groupBy,
        Map<String, String> filters) {

    public SemanticDemoRequest {
        Objects.requireNonNull(question,   "question must not be null");
        Objects.requireNonNull(contractId, "contractId must not be null");
        Objects.requireNonNull(measure,    "measure must not be null");
        Objects.requireNonNull(groupBy,    "groupBy must not be null");
        if (question.isBlank())   throw new IllegalArgumentException("question must not be blank");
        if (contractId.isBlank()) throw new IllegalArgumentException("contractId must not be blank");
        if (measure.isBlank())    throw new IllegalArgumentException("measure must not be blank");
        groupBy = List.copyOf(groupBy);
        filters = filters == null ? Map.of() : Map.copyOf(filters);
    }
}
