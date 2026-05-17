package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * A named, typed metric (measure) defined within a {@link SemanticContract}.
 *
 * <p>A measure is a named aggregation expression — for example
 * {@code SUM(pnl)} named {@code "pnl"}. The {@code expression} field holds
 * the SQL fragment as a plain string descriptor. It is never executed by
 * this record; the semantic compiler (future Lane C story) compiles it into
 * a full query when a {@link SemanticContract} is queried.
 *
 * <p>Measures are referenced by name in semantic queries and dashboard bricks.
 * Renaming a measure is a breaking contract change and requires a major
 * version bump on the containing {@link SemanticContract}.
 *
 * <pre>{@code
 * var pnl = new SemanticMeasure(
 *     "pnl", "Total PnL", "SUM(pnl)", SemanticFieldType.DECIMAL,
 *     "Sum of daily profit and loss", null);
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticMeasure(
        String name,
        String label,
        String expression,
        SemanticFieldType type,
        String description,
        SemanticMeasureExplanation explanation) {

    /**
     * @param name        unique measure identifier within the contract; must not be null or blank
     * @param label       display label for UI and reports; must not be null
     * @param expression  SQL aggregation fragment, e.g. {@code "SUM(pnl)"}; must not be null or blank
     * @param type        result data type; must not be null
     * @param description human-readable description; may be empty but not null
     * @param explanation optional business-facing explanation metadata; may be null
     */
    public SemanticMeasure {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(label, "label must not be null");
        Objects.requireNonNull(expression, "expression must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(description, "description must not be null");
        if (name.isBlank())       throw new IllegalArgumentException("name must not be blank");
        if (expression.isBlank()) throw new IllegalArgumentException("expression must not be blank");
    }
}
