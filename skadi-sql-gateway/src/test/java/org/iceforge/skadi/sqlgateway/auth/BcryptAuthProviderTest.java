package org.iceforge.skadi.sqlgateway.auth;

import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BcryptAuthProviderTest {

    private static final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

    @Test
    void correctPasswordAuthenticated() {
        String hash = encoder.encode("s3cr3t");
        BcryptAuthProvider p = new BcryptAuthProvider(Map.of("alice", hash));
        assertThat(p.authenticate("alice", "s3cr3t")).isTrue();
    }

    @Test
    void wrongPasswordRejected() {
        String hash = encoder.encode("s3cr3t");
        BcryptAuthProvider p = new BcryptAuthProvider(Map.of("alice", hash));
        assertThat(p.authenticate("alice", "wrong")).isFalse();
    }

    @Test
    void unknownUserRejected() {
        String hash = encoder.encode("s3cr3t");
        BcryptAuthProvider p = new BcryptAuthProvider(Map.of("alice", hash));
        assertThat(p.authenticate("bob", "s3cr3t")).isFalse();
    }

    @Test
    void nullPasswordRejected() {
        String hash = encoder.encode("s3cr3t");
        BcryptAuthProvider p = new BcryptAuthProvider(Map.of("alice", hash));
        assertThat(p.authenticate("alice", null)).isFalse();
    }

    @Test
    void requiresPasswordIsTrue() {
        assertThat(new BcryptAuthProvider(Map.of()).requiresPassword()).isTrue();
    }
}
