package org.iceforge.skadi.sqlgateway.auth;

/** Trust-mode provider — accepts any credentials without verification. */
public final class AllowAllAuthProvider implements AuthProvider {

    public static final AllowAllAuthProvider INSTANCE = new AllowAllAuthProvider();

    private AllowAllAuthProvider() {}

    @Override
    public boolean authenticate(String user, String password) {
        return true;
    }

    @Override
    public boolean requiresPassword() {
        return false;
    }
}
