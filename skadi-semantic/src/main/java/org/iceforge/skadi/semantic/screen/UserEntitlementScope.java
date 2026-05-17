package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * Placeholder representing the entitlement scope of the user viewing a widget.
 *
 * <p>This type is a seam for future entitlement enforcement. The buddy chat uses it
 * to contextualise explanation responses — e.g. "You can only see data for legal
 * entities in your entitlement set." Enforcement logic is deferred to a future lane.
 *
 * <p>All fields are required to ensure the buddy chat always has a principal identity.
 * The {@link #entitlementSetId()} is nullable when the user has no scoped entitlement
 * (i.e. full access under the contract's access policy).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record UserEntitlementScope(
        String principalId,
        String entitlementSetId) {

    /**
     * @param principalId      the identity of the requesting user; must not be null or blank
     * @param entitlementSetId the entitlement group/set ID scoping this user's data access;
     *                         may be null when the user has unrestricted access
     */
    public UserEntitlementScope {
        Objects.requireNonNull(principalId, "principalId must not be null");
        if (principalId.isBlank()) throw new IllegalArgumentException("principalId must not be blank");
    }
}
