package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.validation.ContractValidationResult;

import java.util.Objects;

/**
 * The outcome of a {@link ContractRegistryPopulator} run.
 *
 * <p>Callers should always check {@link #isSuccess()} before using {@link #registry()}.
 * The registry is {@code null} when validation produced ERROR issues and population
 * was aborted. Warnings do not prevent population — {@link #isSuccess()} is {@code true}
 * when there are only warnings, and the registry is populated normally.
 *
 * <pre>{@code
 * ContractRegistryPopulationResult result = populator.populateWithResult(contracts);
 * if (result.hasWarnings()) {
 *     result.validationResult().issues().stream()
 *           .filter(i -> i.severity() == WARNING)
 *           .forEach(i -> log.warn("[{}] {}", i.code(), i.message()));
 * }
 * if (!result.isSuccess()) {
 *     throw new IllegalStateException("contract population failed");
 * }
 * ContractRegistry registry = result.registry();
 * }</pre>
 *
 * @param registry            the populated registry; {@code null} if {@link #isSuccess()} is {@code false}
 * @param validationResult    the full validation result including all errors and warnings; never null
 * @param loadedContractCount number of contracts successfully registered; {@code 0} if population failed
 */
public record ContractRegistryPopulationResult(
        ContractRegistry registry,
        ContractValidationResult validationResult,
        int loadedContractCount) {

    /**
     * @param registry            populated registry, or {@code null} when population failed
     * @param validationResult    validation result; must not be null
     * @param loadedContractCount must be &gt;= 0
     */
    public ContractRegistryPopulationResult {
        Objects.requireNonNull(validationResult, "validationResult must not be null");
        if (loadedContractCount < 0)
            throw new IllegalArgumentException("loadedContractCount must be >= 0");
        // registry may be null when validation has errors
    }

    /**
     * Returns {@code true} when validation produced no ERROR issues and the registry
     * was populated successfully. Warnings alone do not cause failure.
     */
    public boolean isSuccess() {
        return !validationResult.hasErrors();
    }

    /** Convenience — delegates to {@link ContractValidationResult#hasWarnings()}. */
    public boolean hasWarnings() {
        return validationResult.hasWarnings();
    }
}
