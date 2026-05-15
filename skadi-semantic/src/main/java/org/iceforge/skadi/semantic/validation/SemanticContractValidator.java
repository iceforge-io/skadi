package org.iceforge.skadi.semantic.validation;

import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.query.SemanticOutputColumn;
import org.iceforge.skadi.semantic.query.SemanticQueryContract;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.iceforge.skadi.semantic.validation.ContractValidationSeverity.ERROR;
import static org.iceforge.skadi.semantic.validation.ContractValidationSeverity.WARNING;
import static org.iceforge.skadi.semantic.validation.ContractValidationIssue.*;

/**
 * Deterministic, offline implementation of {@link ContractValidator}.
 *
 * <p>Also provides extended methods for validating {@link SemanticQueryContract} instances
 * and cross-referencing query contracts against a list of semantic contracts.
 *
 * <h2>Validation rules</h2>
 *
 * <p><strong>Per SemanticContract ({@link #validate(SemanticContract)}):</strong>
 * <ol>
 *   <li>{@link ContractValidationIssue#CONTRACT_DUPLICATE_MEASURE CONTRACT_DUPLICATE_MEASURE} (ERROR)
 *       — two or more measures share the same name</li>
 *   <li>{@link ContractValidationIssue#CONTRACT_DUPLICATE_DIMENSION CONTRACT_DUPLICATE_DIMENSION} (ERROR)
 *       — two or more dimensions share the same name</li>
 *   <li>{@link ContractValidationIssue#CONTRACT_NO_MEASURES CONTRACT_NO_MEASURES} (WARNING)
 *       — the contract defines no measures</li>
 *   <li>{@link ContractValidationIssue#CONTRACT_NO_DIMENSIONS CONTRACT_NO_DIMENSIONS} (WARNING)
 *       — the contract defines no dimensions</li>
 *   <li>{@link ContractValidationIssue#CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED
 *       CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED} (WARNING)
 *       — cache strategy DATASET_VERSION is declared but not yet fully supported</li>
 * </ol>
 *
 * <p><strong>Cross-contract ({@link #validateAll(List)}):</strong>
 * <ol>
 *   <li>All per-contract rules above, in list order</li>
 *   <li>{@link ContractValidationIssue#DUPLICATE_CONTRACT_NAME DUPLICATE_CONTRACT_NAME} (ERROR)
 *       — two or more contracts share the same name</li>
 * </ol>
 *
 * <p><strong>Per SemanticQueryContract ({@link #validate(SemanticQueryContract)}):</strong>
 * <ol>
 *   <li>{@link ContractValidationIssue#QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN
 *       QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN} (ERROR)
 *       — two or more output columns share the same name</li>
 * </ol>
 *
 * <p><strong>Cross-reference ({@link #validateAll(List, List)}):</strong>
 * <ol>
 *   <li>All cross-contract rules for the semantic contract list</li>
 *   <li>All per-query-contract rules, in list order</li>
 *   <li>{@link ContractValidationIssue#QUERY_CONTRACT_UNRESOLVED_SOURCE
 *       QUERY_CONTRACT_UNRESOLVED_SOURCE} (ERROR)
 *       — a query contract's sourceContract name is not in the semantic contract list</li>
 * </ol>
 *
 * <h2>Issue ordering</h2>
 *
 * <p>Issues are always emitted in the following stable order:
 * per-contract issues (in input list order, ERRORs before WARNINGs within each contract),
 * then cross-contract or cross-reference issues at the end.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * var validator = SemanticContractValidator.create();
 * var result = validator.validateAll(contracts);
 * if (!result.isValid()) {
 *     result.issues().forEach(i -> log.warn("[{}] {}: {}", i.severity(), i.code(), i.message()));
 * }
 * }</pre>
 */
public final class SemanticContractValidator implements ContractValidator {

    private SemanticContractValidator() {}

    /** Returns a new, stateless validator instance. */
    public static SemanticContractValidator create() {
        return new SemanticContractValidator();
    }

    // ── ContractValidator ──────────────────────────────────────────────────────

    @Override
    public ContractValidationResult validate(SemanticContract contract) {
        Objects.requireNonNull(contract, "contract must not be null");
        var issues = new ArrayList<ContractValidationIssue>();
        checkDuplicateMeasures(contract, issues);
        checkDuplicateDimensions(contract, issues);
        checkNoMeasures(contract, issues);
        checkNoDimensions(contract, issues);
        checkCachePolicy(contract, issues);
        return new ContractValidationResult(issues);
    }

    @Override
    public ContractValidationResult validateAll(List<SemanticContract> contracts) {
        Objects.requireNonNull(contracts, "contracts must not be null");
        var issues = new ArrayList<ContractValidationIssue>();
        for (var contract : contracts) {
            Objects.requireNonNull(contract, "contracts list must not contain null elements");
            issues.addAll(validate(contract).issues());
        }
        checkDuplicateContractNames(contracts, issues);
        return new ContractValidationResult(issues);
    }

    // ── Extended: SemanticQueryContract ───────────────────────────────────────

    /**
     * Validates a single query contract for internal structural issues.
     *
     * @param queryContract the query contract to validate; must not be null
     * @return validation result; never null
     */
    public ContractValidationResult validate(SemanticQueryContract queryContract) {
        Objects.requireNonNull(queryContract, "queryContract must not be null");
        var issues = new ArrayList<ContractValidationIssue>();
        checkDuplicateOutputColumns(queryContract, issues);
        return new ContractValidationResult(issues);
    }

    /**
     * Validates semantic contracts and query contracts together, including cross-reference
     * checks (a query contract's {@code sourceContract} must name a contract in the list).
     *
     * <p>Issue order: per-contract and cross-contract issues first, then per-query-contract
     * issues and cross-reference issues in query-contract list order.
     *
     * @param contracts      semantic contracts; must not be null; elements must not be null
     * @param queryContracts query contracts; must not be null; elements must not be null
     * @return combined validation result; never null
     */
    public ContractValidationResult validateAll(List<SemanticContract> contracts,
                                                List<SemanticQueryContract> queryContracts) {
        Objects.requireNonNull(contracts,      "contracts must not be null");
        Objects.requireNonNull(queryContracts, "queryContracts must not be null");

        var issues = new ArrayList<ContractValidationIssue>();
        issues.addAll(validateAll(contracts).issues());

        Set<String> contractNames = contracts.stream()
                .map(SemanticContract::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        for (var qc : queryContracts) {
            Objects.requireNonNull(qc, "queryContracts list must not contain null elements");
            issues.addAll(validate(qc).issues());
            if (!contractNames.contains(qc.sourceContract())) {
                issues.add(error(QUERY_CONTRACT_UNRESOLVED_SOURCE,
                        "sourceContract '" + qc.sourceContract()
                                + "' not found in the provided semantic contract list",
                        qc.name(), "sourceContract"));
            }
        }
        return new ContractValidationResult(issues);
    }

    // ── Private checks ─────────────────────────────────────────────────────────

    private static void checkDuplicateMeasures(SemanticContract c,
                                               List<ContractValidationIssue> out) {
        var seen = new LinkedHashSet<String>();
        var duplicates = new LinkedHashSet<String>();
        for (var m : c.measures()) {
            if (!seen.add(m.name())) duplicates.add(m.name());
        }
        for (var dup : duplicates) {
            out.add(error(CONTRACT_DUPLICATE_MEASURE,
                    "duplicate measure name: '" + dup + "'", c.name(), "measures"));
        }
    }

    private static void checkDuplicateDimensions(SemanticContract c,
                                                  List<ContractValidationIssue> out) {
        var seen = new LinkedHashSet<String>();
        var duplicates = new LinkedHashSet<String>();
        for (var d : c.dimensions()) {
            if (!seen.add(d.name())) duplicates.add(d.name());
        }
        for (var dup : duplicates) {
            out.add(error(CONTRACT_DUPLICATE_DIMENSION,
                    "duplicate dimension name: '" + dup + "'", c.name(), "dimensions"));
        }
    }

    private static void checkNoMeasures(SemanticContract c,
                                        List<ContractValidationIssue> out) {
        if (c.measures().isEmpty()) {
            out.add(warning(CONTRACT_NO_MEASURES,
                    "contract defines no measures — add at least one measure or mark as dimension-only",
                    c.name(), "measures"));
        }
    }

    private static void checkNoDimensions(SemanticContract c,
                                          List<ContractValidationIssue> out) {
        if (c.dimensions().isEmpty()) {
            out.add(warning(CONTRACT_NO_DIMENSIONS,
                    "contract defines no dimensions — add at least one dimension or mark as metric-only",
                    c.name(), "dimensions"));
        }
    }

    private static void checkCachePolicy(SemanticContract c,
                                         List<ContractValidationIssue> out) {
        if (c.cachePolicy().strategy() == SemanticCacheStrategy.DATASET_VERSION) {
            out.add(warning(CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED,
                    "cache strategy DATASET_VERSION is not yet supported; "
                            + "contract will be treated as NONE until dataset-version tracking is activated",
                    c.name(), "cachePolicy.strategy"));
        }
    }

    private static void checkDuplicateContractNames(List<SemanticContract> contracts,
                                                    List<ContractValidationIssue> out) {
        var seen = new LinkedHashSet<String>();
        var duplicates = new LinkedHashSet<String>();
        for (var c : contracts) {
            if (!seen.add(c.name())) duplicates.add(c.name());
        }
        for (var dup : duplicates) {
            out.add(error(DUPLICATE_CONTRACT_NAME,
                    "duplicate contract name: '" + dup + "'", dup, null));
        }
    }

    private static void checkDuplicateOutputColumns(SemanticQueryContract qc,
                                                    List<ContractValidationIssue> out) {
        var seen = new LinkedHashSet<String>();
        var duplicates = new LinkedHashSet<String>();
        for (SemanticOutputColumn col : qc.outputShape().columns()) {
            if (!seen.add(col.name())) duplicates.add(col.name());
        }
        for (var dup : duplicates) {
            out.add(error(QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN,
                    "duplicate output column name: '" + dup + "'",
                    qc.name(), "outputShape.columns"));
        }
    }

    // ── Factory helpers ────────────────────────────────────────────────────────

    private static ContractValidationIssue error(String code, String message,
                                                  String contractName, String location) {
        return new ContractValidationIssue(ERROR, code, message, contractName, location);
    }

    private static ContractValidationIssue warning(String code, String message,
                                                    String contractName, String location) {
        return new ContractValidationIssue(WARNING, code, message, contractName, location);
    }
}
