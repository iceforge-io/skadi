package org.iceforge.skadi.semantic.validation;

import java.util.List;
import java.util.Objects;

/**
 * The outcome of validating one or more semantic contracts.
 *
 * <p>The {@link #issues()} list is an unmodifiable, ordered snapshot of all
 * {@link ContractValidationIssue}s produced by a validation run. The list may be empty
 * (no issues found) or contain any mix of {@link ContractValidationSeverity#ERROR} and
 * {@link ContractValidationSeverity#WARNING} items.
 *
 * <h2>Validity vs. warning</h2>
 *
 * <p>{@link #isValid()} returns {@code true} when the result contains no
 * {@link ContractValidationSeverity#ERROR} issues. Warnings are surfaced for
 * human review but do not block registry population or resolution. Callers
 * should log warnings but may proceed with the contract unless they require
 * a warning-free corpus.
 *
 * <pre>{@code
 * ContractValidationResult result = validator.validate(contract);
 * if (!result.isValid()) {
 *     result.issues().stream()
 *           .filter(i -> i.severity() == ContractValidationSeverity.ERROR)
 *           .forEach(i -> log.error("[{}] {} — {}", i.code(), i.contractName(), i.message()));
 *     throw new IllegalStateException("contract validation failed");
 * }
 * }</pre>
 */
public record ContractValidationResult(List<ContractValidationIssue> issues) {

    /**
     * @param issues unmodifiable list of validation issues; must not be null;
     *               copied defensively so the caller's list is not retained
     */
    public ContractValidationResult {
        Objects.requireNonNull(issues, "issues must not be null");
        issues = List.copyOf(issues);
    }

    /** Returns {@code true} if any issue has {@link ContractValidationSeverity#ERROR} severity. */
    public boolean hasErrors() {
        return issues.stream().anyMatch(i -> i.severity() == ContractValidationSeverity.ERROR);
    }

    /** Returns {@code true} if any issue has {@link ContractValidationSeverity#WARNING} severity. */
    public boolean hasWarnings() {
        return issues.stream().anyMatch(i -> i.severity() == ContractValidationSeverity.WARNING);
    }

    /**
     * Returns {@code true} when the result contains no {@link ContractValidationSeverity#ERROR}
     * issues. Warnings alone do not make the result invalid.
     */
    public boolean isValid() {
        return !hasErrors();
    }

    /** Returns a result with no issues — equivalent to {@code new ContractValidationResult(List.of())}. */
    public static ContractValidationResult valid() {
        return new ContractValidationResult(List.of());
    }
}
