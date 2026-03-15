package org.iceforge.skadi.sqlgateway.auth;

/** Pluggable authentication contract for pgwire sessions. */
public interface AuthProvider {

    /**
     * Returns {@code true} if the supplied credentials are valid for {@code user}.
     *
     * <p>Implementations must be thread-safe; a single instance is shared across sessions.
     */
    boolean authenticate(String user, String password);

    /** Returns {@code true} if this provider requires a password from the client. */
    boolean requiresPassword();
}
