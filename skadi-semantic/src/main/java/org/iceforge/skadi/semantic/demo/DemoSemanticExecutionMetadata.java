package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.iceforge.skadi.semantic.service.ExecutionStatus;
import org.iceforge.skadi.semantic.service.SemanticExecutionFailure;

import java.util.Objects;

/**
 * Execution metadata for a completed (or failed) demo semantic query.
 *
 * <p>Carries enough information for a caller to understand what happened — which
 * contract was executed, whether the result came from cache, how many rows were
 * returned, and (if failed) the safe failure classification.
 *
 * <p>No SQL, JDBC URLs, or internal stack traces appear here.
 *
 * <pre>{@code
 * // Successful execution
 * new DemoSemanticExecutionMetadata(
 *     "risk_sensitivity_exposure_grid", "q-001", null, 42L, 210L,
 *     ExecutionStatus.COMPLETED, null);
 *
 * // Cache hit
 * new DemoSemanticExecutionMetadata(
 *     "risk_sensitivity_exposure_grid", "q-002", "FRESH", 42L, null,
 *     ExecutionStatus.CACHE_HIT, null);
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticExecutionMetadata(
        String contractId,
        String queryId,
        String cacheStatus,
        long rowCount,
        Long elapsedMs,
        ExecutionStatus executionStatus,
        SemanticExecutionFailure failureClassification) {

    /**
     * @param contractId             the contract that was executed; must not be null
     * @param queryId                an opaque query identifier for log correlation; must not be null
     * @param cacheStatus            cache state string (e.g. "FRESH", "ABSENT"); null when not applicable
     * @param rowCount               number of rows in the result (0 for failed/unsupported queries)
     * @param elapsedMs              wall-clock execution time in milliseconds; null when unknown
     * @param executionStatus        the terminal execution status; must not be null
     * @param failureClassification  safe failure category; must be null unless status is FAILED
     */
    public DemoSemanticExecutionMetadata {
        Objects.requireNonNull(contractId,       "contractId must not be null");
        Objects.requireNonNull(queryId,          "queryId must not be null");
        Objects.requireNonNull(executionStatus,  "executionStatus must not be null");
        if (executionStatus != ExecutionStatus.FAILED && failureClassification != null) {
            throw new IllegalArgumentException(
                    "failureClassification must be null when status is not FAILED, was: " + executionStatus);
        }
    }
}
