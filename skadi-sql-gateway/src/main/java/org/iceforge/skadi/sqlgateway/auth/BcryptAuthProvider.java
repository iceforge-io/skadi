package org.iceforge.skadi.sqlgateway.auth;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.util.Map;

/**
 * Password-mode provider using BCrypt-hashed credentials.
 *
 * <p>Passwords in config are stored as BCrypt hashes, e.g.:
 * <pre>
 *   users:
 *     alice: $2a$12$...
 * </pre>
 *
 * <p>Generate a hash with:
 * <pre>
 *   import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
 *   System.out.println(new BCryptPasswordEncoder().encode("my-password"));
 * </pre>
 */
public final class BcryptAuthProvider implements AuthProvider {

    private static final BCryptPasswordEncoder ENCODER = new BCryptPasswordEncoder();

    private final Map<String, String> users;

    public BcryptAuthProvider(Map<String, String> users) {
        this.users = users != null ? Map.copyOf(users) : Map.of();
    }

    @Override
    public boolean authenticate(String user, String password) {
        if (user == null || password == null) return false;
        String hash = users.get(user);
        if (hash == null) return false;
        return ENCODER.matches(password, hash);
    }

    @Override
    public boolean requiresPassword() {
        return true;
    }
}
