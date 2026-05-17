package org.iceforge.skadi.semantic.request;

/**
 * Reason codes for a {@link SemanticValidationOutcome}.
 *
 * <p>Each code maps to one explicit outcome category, making validation results
 * programmatically testable and safe for use in user-facing messages without
 * leaking internal details.
 *
 * <h2>Allowed</h2>
 * <ul>
 *   <li>{@link #ALLOWED} — the requested operation is permitted</li>
 * </ul>
 *
 * <h2>Not-found denials</h2>
 * <ul>
 *   <li>{@link #UNKNOWN_CONTRACT} — the requested contract is not in the registry</li>
 *   <li>{@link #UNKNOWN_MEASURE} — the named measure is not defined in the contract</li>
 *   <li>{@link #UNKNOWN_DIMENSION} — the named dimension is not defined in the contract</li>
 * </ul>
 *
 * <h2>Governance denials</h2>
 * <ul>
 *   <li>{@link #DIMENSION_NOT_GROUPABLE} — the dimension exists but groupable is false</li>
 *   <li>{@link #DIMENSION_NOT_FILTERABLE} — the dimension exists but filterable is false</li>
 *   <li>{@link #INVALID_GROUPING_PATH} — the requested combination is not in validGroupingPaths</li>
 *   <li>{@link #UNSUPPORTED_GRAIN} — the requested grain is not supported by this contract</li>
 *   <li>{@link #UNSUPPORTED_OUTPUT_SHAPE} — the requested output shape is not in the contract</li>
 * </ul>
 *
 * <h2>Entitlement (placeholder)</h2>
 * <ul>
 *   <li>{@link #DENIED_BY_ENTITLEMENT} — access denied by entitlement policy; enforcement
 *       is deferred but the seam exists for future use</li>
 * </ul>
 *
 * <h2>Request quality denials</h2>
 * <ul>
 *   <li>{@link #AMBIGUOUS_REQUEST} — the request cannot be resolved to a single subject</li>
 *   <li>{@link #UNSUPPORTED_REQUEST} — the request type is not supported for this contract</li>
 * </ul>
 */
public enum SemanticValidationReasonCode {

    // ── Allowed ────────────────────────────────────────────────────────────────

    /** The requested operation is permitted by the contract and its governance metadata. */
    ALLOWED,

    // ── Not-found denials ──────────────────────────────────────────────────────

    /** The referenced contract name is not registered in the active contract registry. */
    UNKNOWN_CONTRACT,

    /** The referenced measure name is not defined in the named contract. */
    UNKNOWN_MEASURE,

    /** The referenced dimension name is not defined in the named contract. */
    UNKNOWN_DIMENSION,

    // ── Governance denials ─────────────────────────────────────────────────────

    /** The dimension exists but its {@code groupable} flag is {@code false}. */
    DIMENSION_NOT_GROUPABLE,

    /** The dimension exists but its {@code filterable} flag is {@code false}. */
    DIMENSION_NOT_FILTERABLE,

    /**
     * The requested grouping path (combination of dimension names) is not present
     * in the contract's {@code validGroupingPaths} list.
     */
    INVALID_GROUPING_PATH,

    /**
     * The requested grain does not match the grain declared in the contract's
     * explanation metadata.
     */
    UNSUPPORTED_GRAIN,

    /**
     * The requested output shape is not listed in the contract's
     * {@code outputShapes} explanation metadata.
     */
    UNSUPPORTED_OUTPUT_SHAPE,

    // ── Entitlement placeholder ────────────────────────────────────────────────

    /**
     * Access denied by entitlement policy.
     *
     * <p>This is a placeholder seam for future entitlement enforcement. The
     * entitlement behavior model ({@link org.iceforge.skadi.semantic.contract.SemanticEntitlementBehavior})
     * carries the mode and description; policy enforcement is deferred.
     */
    DENIED_BY_ENTITLEMENT,

    // ── Request quality denials ────────────────────────────────────────────────

    /**
     * The request could not be resolved to a single unambiguous subject
     * (e.g. a term matches multiple measures or dimensions).
     */
    AMBIGUOUS_REQUEST,

    /**
     * The request type is not supported for this contract
     * (e.g. grain validation requested but no grain is defined).
     */
    UNSUPPORTED_REQUEST;

    /** Returns {@code true} if this code represents a successful (allowed) outcome. */
    public boolean isAllowed() {
        return this == ALLOWED;
    }
}
