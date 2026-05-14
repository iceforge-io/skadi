package org.iceforge.skadi.semantic.query;

import org.iceforge.skadi.semantic.contract.SemanticFieldType;

import java.util.List;
import java.util.Objects;

/**
 * Descriptor for a single column in a {@link SemanticOutputShape}.
 *
 * <p>A {@code SemanticOutputColumn} describes what a column looks like to a consumer —
 * its identifier, display name, data type, nullability, semantic role, optional
 * formatting hints, and optional lineage/semantic reference pointers.
 *
 * <p>This record is not bound to a live JDBC {@code ResultSet} or any connection.
 * It is a static descriptor that callers can inspect without running a query.
 *
 * <p>{@link #formatHint()} is {@code null} when no display guidance is declared.
 * {@link #references()} is empty when no lineage or semantic pointers are declared.
 *
 * <pre>{@code
 * var pnl = new SemanticOutputColumn(
 *     "pnl", "Total PnL", SemanticFieldType.DECIMAL, true,
 *     SemanticOutputRole.MEASURE,
 *     new SemanticFormatHint("#,##0.00", "GBP", "GBP", 2, 2),
 *     List.of(new SemanticReference("bcbs239", "data-element", "BCBS239-PNL-001", "PnL")));
 * }</pre>
 */
public record SemanticOutputColumn(
        String name,
        String displayName,
        SemanticFieldType fieldType,
        boolean nullable,
        SemanticOutputRole role,
        SemanticFormatHint formatHint,
        List<SemanticReference> references) {

    /**
     * @param name        column identifier used in query results; must not be null or blank
     * @param displayName human-readable label for UI and reports; must not be null
     * @param fieldType   logical data type; must not be null
     * @param nullable    whether the column value may be null in results
     * @param role        semantic role of this column; must not be null
     * @param formatHint  optional display hints; may be null if no hints are declared
     * @param references  lineage or semantic pointers; must not be null; copied defensively
     */
    public SemanticOutputColumn {
        Objects.requireNonNull(name,        "name must not be null");
        Objects.requireNonNull(displayName, "displayName must not be null");
        Objects.requireNonNull(fieldType,   "fieldType must not be null");
        Objects.requireNonNull(role,        "role must not be null");
        Objects.requireNonNull(references,  "references must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
        references = List.copyOf(references);
    }
}
