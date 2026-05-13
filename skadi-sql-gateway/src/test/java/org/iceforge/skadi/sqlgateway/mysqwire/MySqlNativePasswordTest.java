package org.iceforge.skadi.sqlgateway.mysqwire;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B8 — Verifies mysql_native_password challenge-response verification.
 */
class MySqlNativePasswordTest {

    /** Computes what a MySQL client sends: SHA1(password) XOR SHA1(challenge + SHA1(SHA1(password))). */
    private static byte[] clientToken(byte[] challenge, byte[] password) throws Exception {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1pwd = sha1.digest(password);
        sha1.reset();
        byte[] sha1sha1pwd = sha1.digest(sha1pwd);
        sha1.reset();
        sha1.update(challenge);
        sha1.update(sha1sha1pwd);
        byte[] xorMask = sha1.digest();
        byte[] token = new byte[20];
        for (int i = 0; i < 20; i++) token[i] = (byte) (sha1pwd[i] ^ xorMask[i]);
        return token;
    }

    @Test
    void correct_password_verifies_successfully() throws Exception {
        byte[] challenge = new byte[20];
        for (int i = 0; i < 20; i++) challenge[i] = (byte) (i + 1);
        byte[] password = "secret".getBytes(StandardCharsets.UTF_8);
        byte[] token = clientToken(challenge, password);
        assertThat(MySqlWireSession.verifyNativePassword(challenge, password, token)).isTrue();
    }

    @Test
    void wrong_password_fails_verification() throws Exception {
        byte[] challenge = new byte[20];
        for (int i = 0; i < 20; i++) challenge[i] = (byte) (i + 1);
        byte[] correctPassword = "secret".getBytes(StandardCharsets.UTF_8);
        byte[] wrongPassword = "wrong".getBytes(StandardCharsets.UTF_8);
        byte[] token = clientToken(challenge, wrongPassword);
        assertThat(MySqlWireSession.verifyNativePassword(challenge, correctPassword, token)).isFalse();
    }

    @Test
    void empty_token_fails_verification() {
        byte[] challenge = new byte[20];
        byte[] password = "secret".getBytes(StandardCharsets.UTF_8);
        assertThat(MySqlWireSession.verifyNativePassword(challenge, password, new byte[0])).isFalse();
    }

    @Test
    void null_token_fails_verification() {
        byte[] challenge = new byte[20];
        byte[] password = "secret".getBytes(StandardCharsets.UTF_8);
        assertThat(MySqlWireSession.verifyNativePassword(challenge, password, null)).isFalse();
    }

    @Test
    void different_challenges_produce_different_tokens() throws Exception {
        byte[] challenge1 = new byte[20];
        byte[] challenge2 = new byte[20];
        for (int i = 0; i < 20; i++) { challenge1[i] = (byte) (i + 1); challenge2[i] = (byte) (i + 21); }
        byte[] password = "secret".getBytes(StandardCharsets.UTF_8);
        byte[] token1 = clientToken(challenge1, password);
        byte[] token2 = clientToken(challenge2, password);
        assertThat(token1).isNotEqualTo(token2);
        assertThat(MySqlWireSession.verifyNativePassword(challenge1, password, token1)).isTrue();
        assertThat(MySqlWireSession.verifyNativePassword(challenge2, password, token2)).isTrue();
        // Cross-challenge: wrong
        assertThat(MySqlWireSession.verifyNativePassword(challenge1, password, token2)).isFalse();
    }
}
