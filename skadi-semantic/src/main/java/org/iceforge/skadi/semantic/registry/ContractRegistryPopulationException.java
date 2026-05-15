package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.validation.ContractValidationResult;
import org.iceforge.skadi.semantic.validation.ContractValidationSeverity;

import java.util.Objects;

/**
 * Thrown by {@link ContractRegistryPopulator#populate} when the contract list contains
 * one or more {@link ContractValidationSeverity#ERROR} validation issues and therefore
 * cannot be registered.
 *
 * <p>The {@link #validationResult()} method provides structured access to all issues
 * so callers can log or display them without parsing the exception message.
 *
 * <pre>{@code
 * try {
 *     ContractRegistry registry = populator.populate(contracts);
 * } catch (ContractRegistryPopulationException ex) {
 *     ex.validationResult().issues().stream()
 *         .filter(i -> i.severity() == ContractValidationSeverity.ERROR)
 *         .forEach(i -> log.error("[{}] {} — {}", i.code(), i.contractName(), i.message()));
 * }
 * }</pre>
 */
public final class ContractRegistryPopulationException extends RuntimeException {

    private final ContractValidationResult validationResult;

    /**
     * @param validationResult the result containing at least one ERROR issue; must not be null
     */
    public ContractRegistryPopulationException(ContractValidationResult validationResult) {
        super(buildMessage(Objects.requireNonNull(validationResult, "validationResult must not be null")));
        this.validationResult = validationResult;
    }

    /** Returns the structured validation result that caused this exception. Never null. */
    public ContractValidationResult validationResult() {
        return validationResult;
    }

    private static String buildMessage(ContractValidationResult result) {
        long errorCount = result.issues().stream()
                .filter(i -> i.severity() == ContractValidationSeverity.ERROR)
                .count();
        return "registry population failed: " + errorCount
                + " validation error(s) — inspect validationResult() for details";
    }
}
