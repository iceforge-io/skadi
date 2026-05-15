package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.registry.ContractRegistry;

import java.util.Objects;

/**
 * {@link SemanticContractResolver} implementation backed by a {@link ContractRegistry}.
 *
 * <p>Resolution is by contract name only: {@link SemanticResolutionRequest#contractName()}
 * is looked up in the registry. If found, {@link SemanticResolutionResult#found(
 * org.iceforge.skadi.semantic.contract.SemanticContract)} is returned. If not found,
 * {@link SemanticResolutionResult#notFound(String)} is returned with a diagnostic
 * message that includes the contract name and the requesting principal.
 *
 * <h2>Access policy — not enforced in Lane D</h2>
 *
 * <p>The {@link SemanticContractResolver} interface specifies that a contract inaccessible
 * to the requesting principal should be treated as not-found (to prevent information
 * leakage). In Lane D, access policy enforcement is explicitly deferred — all contracts
 * in the registry are considered accessible to any principal. The principal name from
 * {@link SemanticResolutionRequest#context()} is preserved in diagnostic messages but
 * is not used for filtering.
 *
 * <p>When entitlement enforcement is added in a future lane, this class should be
 * extended or replaced with a policy-aware implementation that filters the registry
 * result against the principal's allowed roles and principals from
 * {@link org.iceforge.skadi.semantic.contract.SemanticAccessPolicy}.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * ContractRegistry registry = ContractRegistryPopulator.create().populate(contracts);
 * SemanticContractResolver resolver = RegistrySemanticContractResolver.of(registry);
 *
 * var request = new SemanticResolutionRequest("mxl_risk", ExecutionContext.of("alice"));
 * SemanticResolutionResult result = resolver.resolve(request);
 * if (result.resolved()) {
 *     SemanticContract contract = result.contract();
 * } else {
 *     log.warn("not found: {}", result.errorMessage());
 * }
 * }</pre>
 *
 * <p>This class has no Spring dependencies and does not require a Spring application
 * context. It does not execute SQL, call Databricks, call S3, enforce entitlements,
 * or modify the registry.
 */
public final class RegistrySemanticContractResolver implements SemanticContractResolver {

    private final ContractRegistry registry;

    private RegistrySemanticContractResolver(ContractRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
    }

    /**
     * Returns a resolver backed by the given registry.
     *
     * @param registry the registry to resolve contracts from; must not be null
     */
    public static RegistrySemanticContractResolver of(ContractRegistry registry) {
        return new RegistrySemanticContractResolver(registry);
    }

    /**
     * Resolves a contract by name.
     *
     * <p>Returns {@link SemanticResolutionResult#found} when the registry contains
     * a contract whose name equals {@link SemanticResolutionRequest#contractName()}.
     * Returns {@link SemanticResolutionResult#notFound} otherwise. Never throws for
     * a missing contract — only throws for a null or structurally invalid request
     * (which is already caught by the {@link SemanticResolutionRequest} compact
     * constructor).
     *
     * @param request the resolution request; must not be null
     * @return resolution result; never null
     */
    @Override
    public SemanticResolutionResult resolve(SemanticResolutionRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return registry.findByName(request.contractName())
                .map(SemanticResolutionResult::found)
                .orElseGet(() -> SemanticResolutionResult.notFound(
                        "contract '" + request.contractName() + "' not found"
                                + " (principal='" + request.context().principalName()
                                + "'; access policy not enforced in Lane D)"));
    }
}
