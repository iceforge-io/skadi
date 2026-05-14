package org.iceforge.skadi.semantic.contract;

import java.util.Objects;

/**
 * A reference to a typed security principal used in a {@link SemanticAccessPolicy}.
 *
 * <p>A {@code SemanticPrincipalRef} identifies a specific principal by type
 * (user, group, or service account) and name. It does not resolve the principal
 * against an identity provider, evaluate permissions, or perform any
 * authentication or authorisation. Enforcement is deferred to a future semantic
 * policy enforcer (post-Lane C).
 *
 * <pre>{@code
 * var alice = new SemanticPrincipalRef(SemanticPrincipalType.USER, "alice");
 * var riskTeam = new SemanticPrincipalRef(SemanticPrincipalType.GROUP, "risk-analysts");
 * }</pre>
 */
public record SemanticPrincipalRef(
        SemanticPrincipalType type,
        String name) {

    /**
     * @param type principal type; must not be null
     * @param name principal name or identifier; must not be null or blank
     */
    public SemanticPrincipalRef {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
    }
}
