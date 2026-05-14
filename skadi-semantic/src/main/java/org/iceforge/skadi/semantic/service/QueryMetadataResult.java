package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.query.SemanticOutputShape;
import org.iceforge.skadi.semantic.query.SemanticReference;

import java.util.List;
import java.util.Objects;

/**
 * The result of a {@link QueryMetadataService#inspect} call.
 *
 * <p>{@link #outputShape()} is null when the shape cannot be determined
 * (e.g., SQL-mode inspection for a query the service has not yet seen).
 *
 * <p>{@link #contractName()} identifies the semantic contract the shape was
 * derived from, if any. It is null for SQL-mode results.
 *
 * <p>{@link #references()} carries lineage or semantic pointers associated
 * with this metadata result. The list is unmodifiable.
 */
public record QueryMetadataResult(
        SemanticOutputShape outputShape,
        String contractName,
        List<SemanticReference> references) {

    /**
     * @param outputShape  declared output shape; null if unknown
     * @param contractName name of the source semantic contract; null for SQL-mode
     * @param references   lineage/semantic pointers; must not be null; copied defensively
     */
    public QueryMetadataResult {
        Objects.requireNonNull(references, "references must not be null");
        references = List.copyOf(references);
    }

    /** Returns a result with no shape, no contract, and no references. */
    public static QueryMetadataResult unknown() {
        return new QueryMetadataResult(null, null, List.of());
    }
}
