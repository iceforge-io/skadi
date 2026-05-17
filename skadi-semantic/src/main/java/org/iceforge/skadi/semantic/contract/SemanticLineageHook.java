package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Placeholder value object for lineage hook metadata within a {@link SemanticContractExplanation}.
 *
 * <p>Identifies an upstream lineage hook by ID and provides a human-readable description.
 * All fields are optional; a null value means the information is not specified.
 *
 * <p>This type is a representation-only placeholder. Lineage hook enforcement is out of scope.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticLineageHook(
        String hookId,
        String description) {
}
