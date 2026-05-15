package org.iceforge.skadi.semantic.validation;

import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.util.List;

/**
 * Validates {@link SemanticContract} instances for structural and internal consistency.
 *
 * <p>Validation returns {@link ContractValidationResult} objects — it does not throw for
 * ordinary validation failures. Exceptions are reserved for programmer errors such as
 * null inputs.
 *
 * <p>Implementations must be:
 * <ul>
 *   <li><strong>Deterministic</strong> — same input always produces the same result,
 *       in the same order</li>
 *   <li><strong>Offline</strong> — no Databricks, S3, SQL Gateway, cache, or lineage
 *       calls; no Spring context required</li>
 *   <li><strong>Non-mutating</strong> — does not populate or modify
 *       {@code ContractRegistry}</li>
 *   <li><strong>Non-executing</strong> — does not generate or execute SQL</li>
 * </ul>
 *
 * <p>Cross-contract validation (e.g. duplicate contract names) requires
 * {@link #validateAll}. Single-contract validation is a subset.
 *
 * <p>See {@link SemanticContractValidator} for the concrete implementation and
 * for extended methods that also validate {@code SemanticQueryContract} instances.
 */
public interface ContractValidator {

    /**
     * Validates a single contract for internal structural issues.
     *
     * <p>Checks performed: duplicate measure names, duplicate dimension names,
     * empty measure list (warning), empty dimension list (warning), unsupported
     * cache policy combinations (warning).
     *
     * @param contract the contract to validate; must not be null
     * @return validation result; never null; may contain errors and/or warnings
     */
    ContractValidationResult validate(SemanticContract contract);

    /**
     * Validates a list of contracts for individual issues and cross-contract consistency.
     *
     * <p>Applies {@link #validate} to each contract in list order, then checks for
     * duplicate contract names across the whole list. Issues are ordered:
     * per-contract issues first (in list order), cross-contract issues last.
     *
     * <p>Fails fast on null elements — if any element in {@code contracts} is null,
     * a {@link NullPointerException} is thrown before any validation result is produced.
     *
     * @param contracts list of contracts to validate; must not be null; elements must not be null
     * @return combined validation result; never null
     */
    ContractValidationResult validateAll(List<SemanticContract> contracts);
}
