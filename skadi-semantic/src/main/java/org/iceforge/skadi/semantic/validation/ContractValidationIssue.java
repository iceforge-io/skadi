package org.iceforge.skadi.semantic.validation;

import java.util.Objects;

/**
 * A single diagnostic issue produced by contract validation.
 *
 * <p>Each issue carries:
 * <ul>
 *   <li>{@link #severity()} — whether this is an {@link ContractValidationSeverity#ERROR}
 *       (contract must be fixed) or a {@link ContractValidationSeverity#WARNING}
 *       (operationally suspicious but structurally valid).</li>
 *   <li>{@link #code()} — a stable machine-readable identifier (see constants below).
 *       Codes are UPPER_SNAKE_CASE and should be used in tooling, log filters, and
 *       automated fixers rather than matching on the human-readable message text.</li>
 *   <li>{@link #message()} — a concise, human-readable description of the problem
 *       including the offending name or value where possible.</li>
 *   <li>{@link #contractName()} — the name of the {@code SemanticContract} or
 *       {@code SemanticQueryContract} that produced this issue. For cross-contract
 *       errors such as {@link #DUPLICATE_CONTRACT_NAME}, this is the duplicate name.</li>
 *   <li>{@link #location()} — an optional JSON-path-like pointer to the field that
 *       triggered the issue (e.g. {@code "measures"}, {@code "outputShape.columns"}).
 *       May be {@code null} when the issue is not attributable to a specific field.</li>
 * </ul>
 *
 * <h2>Issue codes</h2>
 *
 * <table border="1">
 *   <tr><th>Code</th><th>Severity</th><th>Meaning</th></tr>
 *   <tr><td>{@link #DUPLICATE_CONTRACT_NAME}</td><td>ERROR</td>
 *       <td>Two or more SemanticContracts share the same name in a validated list</td></tr>
 *   <tr><td>{@link #CONTRACT_DUPLICATE_MEASURE}</td><td>ERROR</td>
 *       <td>Two or more measures in the same contract share a name</td></tr>
 *   <tr><td>{@link #CONTRACT_DUPLICATE_DIMENSION}</td><td>ERROR</td>
 *       <td>Two or more dimensions in the same contract share a name</td></tr>
 *   <tr><td>{@link #CONTRACT_NO_MEASURES}</td><td>WARNING</td>
 *       <td>Contract has no measures — unusual unless it is dimension-only by design</td></tr>
 *   <tr><td>{@link #CONTRACT_NO_DIMENSIONS}</td><td>WARNING</td>
 *       <td>Contract has no dimensions — unusual unless it is metric-only by design</td></tr>
 *   <tr><td>{@link #CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED}</td><td>WARNING</td>
 *       <td>Cache strategy DATASET_VERSION is a future feature; TTL will be used instead</td></tr>
 *   <tr><td>{@link #QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN}</td><td>ERROR</td>
 *       <td>Two or more output columns in the same SemanticQueryContract share a name</td></tr>
 *   <tr><td>{@link #QUERY_CONTRACT_UNRESOLVED_SOURCE}</td><td>ERROR</td>
 *       <td>A SemanticQueryContract references a sourceContract name not found in the
 *           provided SemanticContract list</td></tr>
 * </table>
 */
public record ContractValidationIssue(
        ContractValidationSeverity severity,
        String code,
        String message,
        String contractName,
        String location) {

    // ── Issue codes ────────────────────────────────────────────────────────────

    /** Two or more {@code SemanticContract}s share the same name in a validated list. */
    public static final String DUPLICATE_CONTRACT_NAME = "DUPLICATE_CONTRACT_NAME";

    /** Two or more measures within the same contract share a name. */
    public static final String CONTRACT_DUPLICATE_MEASURE = "CONTRACT_DUPLICATE_MEASURE";

    /** Two or more dimensions within the same contract share a name. */
    public static final String CONTRACT_DUPLICATE_DIMENSION = "CONTRACT_DUPLICATE_DIMENSION";

    /** The contract defines no measures. */
    public static final String CONTRACT_NO_MEASURES = "CONTRACT_NO_MEASURES";

    /** The contract defines no dimensions. */
    public static final String CONTRACT_NO_DIMENSIONS = "CONTRACT_NO_DIMENSIONS";

    /**
     * Cache strategy {@code DATASET_VERSION} is declared. This is a future feature —
     * the loader will treat it as {@code NONE} until dataset-version tracking is activated.
     */
    public static final String CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED =
            "CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED";

    /** Two or more output columns in a {@code SemanticQueryContract} share a name. */
    public static final String QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN =
            "QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN";

    /**
     * A {@code SemanticQueryContract}'s {@code sourceContract} name does not match
     * any {@code SemanticContract} in the validated list.
     */
    public static final String QUERY_CONTRACT_UNRESOLVED_SOURCE =
            "QUERY_CONTRACT_UNRESOLVED_SOURCE";

    // ── Compact constructor ────────────────────────────────────────────────────

    /**
     * @param severity     issue level; must not be null
     * @param code         stable machine-readable identifier; must not be null or blank
     * @param message      human-readable description; must not be null
     * @param contractName name of the contract that produced this issue; must not be null
     * @param location     optional field path (e.g. {@code "measures"}); may be null
     */
    public ContractValidationIssue {
        Objects.requireNonNull(severity,     "severity must not be null");
        Objects.requireNonNull(code,         "code must not be null");
        Objects.requireNonNull(message,      "message must not be null");
        Objects.requireNonNull(contractName, "contractName must not be null");
        if (code.isBlank()) throw new IllegalArgumentException("code must not be blank");
        // location is nullable
    }
}
