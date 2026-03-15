package org.iceforge.skadi.sqlgateway.auth;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthProviderFactoryTest {

    private static SqlGatewayProperties.PgWire.Auth auth(String mode, String store) {
        return new SqlGatewayProperties.PgWire.Auth(mode, store, Map.of(), null);
    }

    @Test
    void nullAuthGivesAllowAll() {
        assertThat(AuthProviderFactory.create(null)).isInstanceOf(AllowAllAuthProvider.class);
    }

    @Test
    void trustModeGivesAllowAll() {
        assertThat(AuthProviderFactory.create(auth("trust", null))).isInstanceOf(AllowAllAuthProvider.class);
    }

    @Test
    void nullModeGivesAllowAll() {
        assertThat(AuthProviderFactory.create(auth(null, null))).isInstanceOf(AllowAllAuthProvider.class);
    }

    @Test
    void passwordModePlaintextGivesPlaintext() {
        assertThat(AuthProviderFactory.create(auth("password", "plaintext"))).isInstanceOf(PlaintextAuthProvider.class);
    }

    @Test
    void passwordModeDefaultGivesPlaintext() {
        assertThat(AuthProviderFactory.create(auth("password", null))).isInstanceOf(PlaintextAuthProvider.class);
    }

    @Test
    void passwordModeBcryptGivesBcrypt() {
        assertThat(AuthProviderFactory.create(auth("password", "bcrypt"))).isInstanceOf(BcryptAuthProvider.class);
    }

    @Test
    void unknownModeThrows() {
        assertThatThrownBy(() -> AuthProviderFactory.create(auth("kerberos", null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kerberos");
    }
}
