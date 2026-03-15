package org.iceforge.skadi.sqlgateway.auth;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;

/** Creates the appropriate {@link AuthProvider} from gateway configuration. */
public final class AuthProviderFactory {

    private AuthProviderFactory() {}

    public static AuthProvider create(SqlGatewayProperties.PgWire.Auth auth) {
        if (auth == null) return AllowAllAuthProvider.INSTANCE;

        String mode = auth.mode();
        if (mode == null || mode.equalsIgnoreCase("trust")) {
            return AllowAllAuthProvider.INSTANCE;
        }

        if (mode.equalsIgnoreCase("password")) {
            String store = auth.credentialStore();
            if ("bcrypt".equalsIgnoreCase(store)) {
                return new BcryptAuthProvider(auth.users());
            }
            // Default: plaintext (backward compat)
            return new PlaintextAuthProvider(auth.users());
        }

        throw new IllegalArgumentException("Unknown auth mode: " + mode
                + ". Supported modes: trust, password");
    }
}
