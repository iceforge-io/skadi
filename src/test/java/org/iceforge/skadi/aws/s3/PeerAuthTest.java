package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PeerAuthTest {

    private PeerAuth auth;
    private final String keyId = "k1";
    private final String secret = "supersecret";
    private final String method = "GET";
    private final String path = "/api/resource";
    private final String query = "a=1&b=2";

    @BeforeEach
    void setup() {
        auth = new PeerAuth();
        ReflectionTestUtils.setField(auth, "allowedSkewSeconds", 30L);
        Map<String, String> secrets = new HashMap<>();
        secrets.put(keyId, secret);
        ReflectionTestUtils.setField(auth, "sharedSecrets", secrets);
    }

    private String sig(String ts, String nonce) {
        String canonical = method + "\n" + path + "\n" + query + "\n" + ts + "\n" + nonce;
        return invokeHmac(secret, canonical);
    }

    private String invokeHmac(String s, String d) {
        try {
            var method = PeerAuth.class.getDeclaredMethod("hmacB64Url", String.class, String.class);
            method.setAccessible(true);
            return (String) method.invoke(null, s, d);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void verify_ok() {
        String ts = String.valueOf(Instant.now().toEpochMilli());
        String nonce = "n1";
        String sig = sig(ts, nonce);

        assertDoesNotThrow(() -> auth.verify(method, path, query, keyId, ts, nonce, sig));
    }

    @Test
    void verify_badSignature() {
        String ts = String.valueOf(Instant.now().toEpochMilli());
        String nonce = "n2";
        String sig = "bad";

        assertThrows(PeerAuth.Unauthorized.class,
                () -> auth.verify(method, path, query, keyId, ts, nonce, sig));
    }

    @Test
    void verify_skewTooLarge() {
        long past = Instant.now().toEpochMilli() - (60_000L); // 60s
        String ts = String.valueOf(past);
        String nonce = "n3";
        String sig = sig(ts, nonce);

        assertThrows(PeerAuth.Unauthorized.class,
                () -> auth.verify(method, path, query, keyId, ts, nonce, sig));
    }

    @Test
    void verify_replayNonce() {
        String ts = String.valueOf(Instant.now().toEpochMilli());
        String nonce = "n4";
        String sig = sig(ts, nonce);

        // First call OK
        auth.verify(method, path, query, keyId, ts, nonce, sig);
        // Second call with same nonce must fail
        assertThrows(PeerAuth.Unauthorized.class,
                () -> auth.verify(method, path, query, keyId, ts, nonce, sig));
    }

    @Test
    void verify_missingConfig() {
        // Empty secrets -> unauthorized
        ReflectionTestUtils.setField(auth, "sharedSecrets", new HashMap<>());

        String ts = String.valueOf(Instant.now().toEpochMilli());
        String nonce = "n5";

        assertThrows(PeerAuth.Unauthorized.class,
                () -> auth.verify(method, path, query, keyId, ts, nonce, "x"));
    }

    @Test
    void constantTimeEquals_works() throws Exception {
        var m = PeerAuth.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
        m.setAccessible(true);
        assertTrue((Boolean) m.invoke(null, "abc", "abc"));
        assertFalse((Boolean) m.invoke(null, "abc", "abd"));
        assertFalse((Boolean) m.invoke(null, "abc", "ab"));
        assertFalse((Boolean) m.invoke(null, null, "ab"));
    }
}