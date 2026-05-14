package org.iceforge.skadi.semantic.contract;

import java.util.List;
import java.util.Objects;

/**
 * Descriptive access policy metadata for a {@link SemanticContract}.
 *
 * <p>An {@code SemanticAccessPolicy} declares which roles, principals, and
 * governance rules apply to a contract. It is a descriptor only — it does not
 * enforce access, resolve role membership, or communicate with any identity
 * provider. Enforcement is the responsibility of a future semantic policy
 * enforcer (post-Lane C).
 *
 * <p>All three lists are defensively copied on construction and are unmodifiable.
 * An empty list means no restriction of that kind is declared (not that access
 * is denied — the semantics of empty lists are defined by the policy enforcer,
 * not by this record).
 *
 * <p>Use {@link #unrestricted()} for a contract with no declared restrictions:
 * <pre>{@code
 * var policy = SemanticAccessPolicy.unrestricted();
 * }</pre>
 *
 * <p>Use the canonical constructor to express specific access declarations:
 * <pre>{@code
 * var policy = new SemanticAccessPolicy(
 *     List.of(new SemanticRoleRef("risk_analyst")),
 *     List.of(new SemanticPrincipalRef(SemanticPrincipalType.USER, "alice")),
 *     List.of());
 * }</pre>
 */
public record SemanticAccessPolicy(
        List<SemanticRoleRef> allowedRoles,
        List<SemanticPrincipalRef> allowedPrincipals,
        List<SemanticRuleRef> ruleRefs) {

    /**
     * @param allowedRoles       role references declared as allowed; must not be null;
     *                           copied defensively
     * @param allowedPrincipals  principal references declared as allowed; must not be null;
     *                           copied defensively
     * @param ruleRefs           governance rule references that apply to this policy;
     *                           must not be null; copied defensively
     */
    public SemanticAccessPolicy {
        Objects.requireNonNull(allowedRoles, "allowedRoles must not be null");
        Objects.requireNonNull(allowedPrincipals, "allowedPrincipals must not be null");
        Objects.requireNonNull(ruleRefs, "ruleRefs must not be null");
        allowedRoles      = List.copyOf(allowedRoles);
        allowedPrincipals = List.copyOf(allowedPrincipals);
        ruleRefs          = List.copyOf(ruleRefs);
    }

    /**
     * Returns a policy with no declared role, principal, or rule restrictions.
     *
     * <p>The semantics of "no restrictions" (allow-all vs. default-deny) are
     * determined by the policy enforcer, not by this record.
     */
    public static SemanticAccessPolicy unrestricted() {
        return new SemanticAccessPolicy(List.of(), List.of(), List.of());
    }

    /** Returns {@code true} if no roles, principals, or rules are declared. */
    public boolean isEmpty() {
        return allowedRoles.isEmpty() && allowedPrincipals.isEmpty() && ruleRefs.isEmpty();
    }
}
