package org.iceforge.skadi.semantic.contract;

import java.util.Objects;

/**
 * A reference to a named role used in a {@link SemanticAccessPolicy}.
 *
 * <p>A {@code SemanticRoleRef} carries a role name only — it does not resolve
 * role membership, evaluate permissions, or interact with any identity provider.
 * Role enforcement is deferred to a future semantic policy enforcer (post-Lane C).
 *
 * <pre>{@code
 * var analyst = new SemanticRoleRef("risk_analyst");
 * }</pre>
 */
public record SemanticRoleRef(String name) {

    /**
     * @param name role name; must not be null or blank
     */
    public SemanticRoleRef {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
    }
}
