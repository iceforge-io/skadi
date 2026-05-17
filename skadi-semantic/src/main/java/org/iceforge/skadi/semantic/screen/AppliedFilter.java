package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * A filter currently constraining the widget's data.
 *
 * <p>The {@link #dimensionId()} must match a dimension with {@code filterable=true}
 * in the governing contract. The {@link #operator()} and {@link #value()} describe
 * the predicate applied (e.g. {@code "equals"}, {@code "gte"}, {@code "in"}).
 *
 * <p>Values are represented as strings for serialization; the semantic layer
 * interprets them according to the dimension's declared type.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AppliedFilter(
        String dimensionId,
        String operator,
        String value) {

    /**
     * @param dimensionId the filterable dimension name; must not be null or blank
     * @param operator    the filter operator (e.g. {@code "equals"}, {@code "gte"}); must not be null or blank
     * @param value       the filter value as a string; must not be null
     */
    public AppliedFilter {
        Objects.requireNonNull(dimensionId, "dimensionId must not be null");
        Objects.requireNonNull(operator,    "operator must not be null");
        Objects.requireNonNull(value,       "value must not be null");
        if (dimensionId.isBlank()) throw new IllegalArgumentException("dimensionId must not be blank");
        if (operator.isBlank())    throw new IllegalArgumentException("operator must not be blank");
    }
}
