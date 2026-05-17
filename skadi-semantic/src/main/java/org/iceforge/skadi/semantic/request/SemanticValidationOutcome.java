package org.iceforge.skadi.semantic.request;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

/**
 * The outcome of a semantic request validation.
 *
 * <p>{@link #allowed()} is the primary decision: {@code true} means the operation
 * is permitted, {@code false} means it is denied. {@link #reasonCode()} gives the
 * specific machine-readable reason, and {@link #message()} provides a user-safe
 * explanation suitable for display in buddy-chat or dashboard UI.
 *
 * <p>Optional fields ({@link #contractId()}, {@link #invalidFields()}) carry
 * diagnostic detail when available.
 *
 * <p>Use the static factories rather than constructing directly:
 * <ul>
 *   <li>{@link #allowed(String, String)} — permitted outcome</li>
 *   <li>{@link #denied(SemanticValidationReasonCode, String)} — denied with reason</li>
 *   <li>{@link #denied(SemanticValidationReasonCode, String, String, List)} — denied with context</li>
 * </ul>
 *
 * <pre>{@code
 * // Permitted
 * SemanticValidationOutcome.allowed("cib_var", "legal_entity can be used as a GROUP BY dimension.");
 *
 * // Denied — dimension exists but is not groupable
 * SemanticValidationOutcome.denied(
 *     SemanticValidationReasonCode.DIMENSION_NOT_GROUPABLE,
 *     "The 'desk' dimension is not available for grouping in regulatory reports.",
 *     "cib_var",
 *     List.of("desk"));
 *
 * // Denied — entitlement placeholder
 * SemanticValidationOutcome.denied(
 *     SemanticValidationReasonCode.DENIED_BY_ENTITLEMENT,
 *     "You do not have access to cross-entity aggregation for this contract.");
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticValidationOutcome(
        boolean allowed,
        SemanticValidationReasonCode reasonCode,
        String message,
        String contractId,
        List<String> invalidFields) {

    /**
     * @param allowed       {@code true} if the operation is permitted
     * @param reasonCode    the machine-readable reason; must not be null
     * @param message       user-safe explanation; must not be null
     * @param contractId    the contract this outcome applies to; may be null
     * @param invalidFields the specific fields that caused denial; may be null;
     *                      defensively copied when non-null
     */
    public SemanticValidationOutcome {
        Objects.requireNonNull(reasonCode, "reasonCode must not be null");
        Objects.requireNonNull(message,    "message must not be null");
        if (allowed && !reasonCode.isAllowed()) {
            throw new IllegalArgumentException(
                    "allowed outcome must use ALLOWED reason code, got: " + reasonCode);
        }
        if (!allowed && reasonCode.isAllowed()) {
            throw new IllegalArgumentException(
                    "denied outcome must not use ALLOWED reason code");
        }
        invalidFields = invalidFields == null ? null : List.copyOf(invalidFields);
    }

    // ── Static factories ───────────────────────────────────────────────────────

    /**
     * Returns an allowed outcome for the given contract.
     *
     * @param contractId the contract that was checked; must not be null
     * @param message    user-safe explanation of why the operation is allowed
     */
    public static SemanticValidationOutcome allowed(String contractId, String message) {
        Objects.requireNonNull(contractId, "contractId must not be null");
        return new SemanticValidationOutcome(
                true, SemanticValidationReasonCode.ALLOWED, message, contractId, null);
    }

    /**
     * Returns a denied outcome with no contract or field context.
     *
     * @param reasonCode the reason for denial; must not be null and must not be ALLOWED
     * @param message    user-safe explanation of why the operation is denied
     */
    public static SemanticValidationOutcome denied(SemanticValidationReasonCode reasonCode,
                                                   String message) {
        return new SemanticValidationOutcome(false, reasonCode, message, null, null);
    }

    /**
     * Returns a denied outcome with contract and invalid-field context.
     *
     * @param reasonCode    the reason for denial; must not be null and must not be ALLOWED
     * @param message       user-safe explanation of why the operation is denied
     * @param contractId    the contract that was checked; may be null
     * @param invalidFields the fields that triggered the denial; may be null
     */
    public static SemanticValidationOutcome denied(SemanticValidationReasonCode reasonCode,
                                                   String message,
                                                   String contractId,
                                                   List<String> invalidFields) {
        return new SemanticValidationOutcome(false, reasonCode, message, contractId, invalidFields);
    }
}
