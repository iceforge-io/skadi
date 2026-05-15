package org.iceforge.skadi.semantic.validation;

/**
 * Severity level for a {@link ContractValidationIssue}.
 *
 * <ul>
 *   <li>{@link #ERROR} — the contract is structurally or referentially invalid and must
 *       not be registered or used for query resolution until fixed.</li>
 *   <li>{@link #WARNING} — the contract is structurally valid but operationally suspicious
 *       (e.g., no measures, unsupported future feature). It may still be registered,
 *       but the warning should be surfaced to the contract author.</li>
 * </ul>
 *
 * <p>A {@link ContractValidationResult} is considered valid ({@code isValid() == true})
 * when it contains no {@link #ERROR} issues. Warnings alone do not make a result invalid.
 */
public enum ContractValidationSeverity {
    ERROR,
    WARNING
}
