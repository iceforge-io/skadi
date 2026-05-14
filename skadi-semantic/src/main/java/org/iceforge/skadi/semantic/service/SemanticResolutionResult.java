package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.util.Objects;

/**
 * The result of a {@link SemanticContractResolver#resolve} call.
 *
 * <p>{@link #resolved()} is {@code true} when the contract was found and
 * the calling principal has access to it. {@link #contract()} is non-null
 * in this case.
 *
 * <p>{@link #resolved()} is {@code false} when the contract was not found
 * or the principal does not have access. In either case {@link #contract()}
 * is null (access-denied is not distinguished from not-found to avoid
 * information leakage).
 *
 * <p>{@link #errorMessage()} is null when resolved is {@code true} and
 * may carry a diagnostic message when resolved is {@code false}.
 */
public record SemanticResolutionResult(
        SemanticContract contract,
        boolean resolved,
        String errorMessage) {

    /**
     * @param contract     the resolved contract; must be non-null when resolved is true
     * @param resolved     true if the contract was found and accessible
     * @param errorMessage optional diagnostic; should be null when resolved is true
     */
    public SemanticResolutionResult {
        if (resolved && contract == null) {
            throw new IllegalArgumentException("contract must not be null when resolved is true");
        }
        if (!resolved && contract != null) {
            throw new IllegalArgumentException("contract must be null when resolved is false");
        }
    }

    /**
     * Returns a successful resolution result.
     *
     * @param contract the resolved contract; must not be null
     */
    public static SemanticResolutionResult found(SemanticContract contract) {
        Objects.requireNonNull(contract, "contract must not be null");
        return new SemanticResolutionResult(contract, true, null);
    }

    /**
     * Returns a not-found result with an optional diagnostic message.
     *
     * @param message optional diagnostic; may be null
     */
    public static SemanticResolutionResult notFound(String message) {
        return new SemanticResolutionResult(null, false, message);
    }
}
