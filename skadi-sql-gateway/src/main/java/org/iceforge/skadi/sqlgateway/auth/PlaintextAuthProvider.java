package org.iceforge.skadi.sqlgateway.auth;

import java.util.Map;

/**
 * Password-mode provider using plaintext credentials from config.
 * Preserved for backward compatibility; prefer {@link BcryptAuthProvider} for production.
 */
public final class PlaintextAuthProvider implements AuthProvider {

    private final Map<String, String> users;

    public PlaintextAuthProvider(Map<String, String> users) {
        this.users = users != null ? Map.copyOf(users) : Map.of();
    }

    @Override
    public boolean authenticate(String user, String password) {
        if (user == null || password == null) return false;
        String expected = users.get(user);
        return expected != null && expected.equals(password);
    }

    @Override
    public boolean requiresPassword() {
        return true;
    }
}
