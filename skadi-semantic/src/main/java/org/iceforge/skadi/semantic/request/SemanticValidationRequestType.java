package org.iceforge.skadi.semantic.request;

/**
 * Classifies the kind of semantic operation being validated in a
 * {@link SemanticValidationRequest}.
 */
public enum SemanticValidationRequestType {

    /** Validate whether a named measure is accessible and defined in the contract. */
    MEASURE_ACCESS,

    /** Validate whether a named dimension may be used as a GROUP BY dimension. */
    DIMENSION_GROUPING,

    /** Validate whether a named dimension may be used as a filter predicate. */
    DIMENSION_FILTER,

    /** Validate whether a comma-joined path of dimension names is a valid grouping combination. */
    GROUPING_PATH,

    /** Validate whether a named output shape is declared for this contract. */
    OUTPUT_SHAPE,

    /** Validate whether a stated grain is consistent with the contract's grain declaration. */
    GRAIN
}
