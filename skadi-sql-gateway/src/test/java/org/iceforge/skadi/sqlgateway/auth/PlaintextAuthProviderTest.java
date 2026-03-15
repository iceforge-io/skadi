package org.iceforge.skadi.sqlgateway.auth;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PlaintextAuthProviderTest {

    @Test
    void correctPasswordAuthenticated() {
        PlaintextAuthProvider p = new PlaintextAuthProvider(Map.of("alice", "secret"));
        assertThat(p.authenticate("alice", "secret")).isTrue();
    }

    @Test
    void wrongPasswordRejected() {
        PlaintextAuthProvider p = new PlaintextAuthProvider(Map.of("alice", "secret"));
        assertThat(p.authenticate("alice", "wrong")).isFalse();
    }

    @Test
    void unknownUserRejected() {
        PlaintextAuthProvider p = new PlaintextAuthProvider(Map.of("alice", "secret"));
        assertThat(p.authenticate("bob", "secret")).isFalse();
    }

    @Test
    void nullPasswordRejected() {
        PlaintextAuthProvider p = new PlaintextAuthProvider(Map.of("alice", "secret"));
        assertThat(p.authenticate("alice", null)).isFalse();
    }

    @Test
    void emptyUsersMapRejectsAll() {
        PlaintextAuthProvider p = new PlaintextAuthProvider(Map.of());
        assertThat(p.authenticate("alice", "any")).isFalse();
    }

    @Test
    void requiresPasswordIsTrue() {
        assertThat(new PlaintextAuthProvider(Map.of()).requiresPassword()).isTrue();
    }
}
