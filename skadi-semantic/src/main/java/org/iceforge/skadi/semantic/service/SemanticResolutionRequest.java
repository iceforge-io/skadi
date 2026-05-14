package org.iceforge.skadi.semantic.service;

import java.util.Objects;

/**
 * A request to resolve a {@code SemanticContract} by name via
 * {@link SemanticContractResolver}.
 *
 * <p>The resolver applies principal access policy from the context —
 * a contract that the principal is not permitted to access is treated as
 * not found, not as an access-denied error, to avoid information leakage.
 */
public record SemanticResolutionRequest(
        String contractName,
        ExecutionContext context) {

    /**
     * @param contractName the name of the contract to resolve; must not be null or blank
     * @param context      execution context carrying principal identity; must not be null
     */
    public SemanticResolutionRequest {
        Objects.requireNonNull(contractName, "contractName must not be null");
        Objects.requireNonNull(context,      "context must not be null");
        if (contractName.isBlank()) {
            throw new IllegalArgumentException("contractName must not be blank");
        }
    }
}
