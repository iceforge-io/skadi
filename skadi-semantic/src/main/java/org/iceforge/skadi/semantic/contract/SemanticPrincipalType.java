package org.iceforge.skadi.semantic.contract;

/**
 * Classification of a security principal that may appear in a
 * {@link SemanticAccessPolicy}.
 *
 * <p>This enum is a descriptor only — it carries no authentication or
 * authorization logic. Enforcement happens in a future semantic policy
 * enforcer (post-Lane C), not here.
 */
public enum SemanticPrincipalType {

    /** An individual human user identity. */
    USER,

    /** A named group of users (e.g. an LDAP/AD group). */
    GROUP,

    /** A non-human service account or application identity. */
    SERVICE_ACCOUNT
}
