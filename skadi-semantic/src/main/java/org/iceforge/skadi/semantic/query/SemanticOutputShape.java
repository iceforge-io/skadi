package org.iceforge.skadi.semantic.query;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The output shape of a query: an ordered list of {@link SemanticOutputColumn}s
 * with optional metadata.
 *
 * <p>A {@code SemanticOutputShape} describes what a query returns without
 * executing it. Consumers use the shape to:
 * <ul>
 *   <li>Pre-allocate result buffers ({@link #rowCountHint()})</li>
 *   <li>Build column headers for UI tables or reports</li>
 *   <li>Map semantic roles to axis bindings in charts</li>
 *   <li>Provide AI context about expected result structure</li>
 * </ul>
 *
 * <p>Column order in {@link #columns()} is significant — it matches the order in
 * which columns are expected to appear in query results.
 *
 * <p>{@link #rowCountHint()} is {@code null} when no estimate is available.
 * Consumers must not rely on it for correctness — it is advisory only.
 *
 * <pre>{@code
 * var shape = new SemanticOutputShape(
 *     List.of(cobDateCol, bookCol, pnlCol),
 *     1000L,
 *     List.of());
 * }</pre>
 */
public record SemanticOutputShape(
        List<SemanticOutputColumn> columns,
        Long rowCountHint,
        List<SemanticReference> references) {

    /**
     * @param columns       ordered output columns; must not be null; copied defensively
     * @param rowCountHint  advisory estimated row count; may be null when unknown
     * @param references    shape-level lineage or semantic pointers; must not be null;
     *                      copied defensively
     */
    public SemanticOutputShape {
        Objects.requireNonNull(columns,    "columns must not be null");
        Objects.requireNonNull(references, "references must not be null");
        columns    = List.copyOf(columns);
        references = List.copyOf(references);
    }

    /**
     * Convenience constructor with no row-count hint and no shape-level references.
     *
     * @param columns ordered output columns; must not be null
     */
    public SemanticOutputShape(List<SemanticOutputColumn> columns) {
        this(columns, null, List.of());
    }

    /**
     * Finds a column by its {@link SemanticOutputColumn#name()} (case-sensitive).
     *
     * @param name column name to look up
     * @return the column, or {@link Optional#empty()} if not found
     */
    public Optional<SemanticOutputColumn> findColumn(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return columns.stream().filter(c -> c.name().equals(name)).findFirst();
    }

    /** Returns the number of output columns. */
    public int columnCount() {
        return columns.size();
    }
}
