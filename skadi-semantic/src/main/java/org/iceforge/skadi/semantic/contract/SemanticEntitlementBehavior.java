package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Placeholder value object for entitlement behavior metadata within a
 * {@link SemanticContractExplanation}.
 *
 * <p>Describes how access entitlements are applied to this contract.
 * All fields are optional; a null value means the information is not specified.
 *
 * <p>This type is a representation-only placeholder. Entitlement enforcement is out of scope.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticEntitlementBehavior(
        String mode,
        String description) {
}
