package org.iceforge.skadi.semantic.contract;

import java.util.Objects;

/**
 * A named grouping or filtering dimension defined within a {@link SemanticContract}.
 *
 * <p>A dimension maps a logical name (e.g. {@code "book"}) to a physical
 * column name (e.g. {@code "book"} or {@code "trading_book"}) and declares
 * whether callers may filter by it, group by it, or both.
 *
 * <p>The {@link #filterable()} and {@link #groupable()} flags are declarative
 * metadata — the semantic compiler (future Lane C story) uses them to validate
 * incoming queries. This record does not enforce them at runtime.
 *
 * <pre>{@code
 * var book = new SemanticDimension(
 *     "book", "book", SemanticFieldType.STRING,
 *     "Trading Book", true, true);
 * }</pre>
 */
public record SemanticDimension(
        String name,
        String column,
        SemanticFieldType type,
        String label,
        boolean filterable,
        boolean groupable) {

    /**
     * @param name       unique dimension identifier within the contract; must not be null or blank
     * @param column     physical column name in the backing table; must not be null or blank
     * @param type       data type of the column; must not be null
     * @param label      display label for UI and reports; must not be null
     * @param filterable whether callers may use this dimension as a filter
     * @param groupable  whether callers may group by this dimension
     */
    public SemanticDimension {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(column, "column must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(label, "label must not be null");
        if (name.isBlank())   throw new IllegalArgumentException("name must not be blank");
        if (column.isBlank()) throw new IllegalArgumentException("column must not be blank");
    }
}
