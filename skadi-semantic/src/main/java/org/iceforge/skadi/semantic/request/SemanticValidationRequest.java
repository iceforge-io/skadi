package org.iceforge.skadi.semantic.request;

import java.util.Objects;

/**
 * A request to validate whether a specific semantic operation is permitted
 * under the governed semantic model.
 *
 * <p>Validation is contract-scoped: the caller names a contract and a subject
 * (e.g. a measure name, dimension name, grouping path) along with the type of
 * operation being requested. The validation service checks the contract's
 * governance metadata and returns a {@link SemanticValidationOutcome}.
 *
 * <p>This record is the input to validation only — it does not execute SQL,
 * call Databricks, or query any cache.
 *
 * <p>Examples:
 * <pre>{@code
 * // Can I group by desk?
 * new SemanticValidationRequest("cib_var", SemanticValidationRequestType.DIMENSION_GROUPING, "desk");
 *
 * // Can I filter by legal entity?
 * new SemanticValidationRequest("cib_var", SemanticValidationRequestType.DIMENSION_FILTER, "legal_entity");
 *
 * // Is this grouping path valid?
 * new SemanticValidationRequest("cib_var", SemanticValidationRequestType.GROUPING_PATH, "cob_date,legal_entity");
 * }</pre>
 */
public record SemanticValidationRequest(
        String contractId,
        SemanticValidationRequestType requestType,
        String subjectId) {

    /**
     * @param contractId  the name of the contract to validate against; must not be null or blank
     * @param requestType the kind of operation being validated; must not be null
     * @param subjectId   the subject of the validation (measure name, dimension name,
     *                    grouping path, output shape name, or grain string);
     *                    must not be null or blank
     */
    public SemanticValidationRequest {
        Objects.requireNonNull(contractId,   "contractId must not be null");
        Objects.requireNonNull(requestType,  "requestType must not be null");
        Objects.requireNonNull(subjectId,    "subjectId must not be null");
        if (contractId.isBlank())  throw new IllegalArgumentException("contractId must not be blank");
        if (subjectId.isBlank())   throw new IllegalArgumentException("subjectId must not be blank");
    }
}
