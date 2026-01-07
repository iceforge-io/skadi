package org.iceforge.skadi.aws.s3;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PeerAuth {

    @Value("${skadi.peerCache.auth.allowedSkewSeconds:30}")
    private long allowedSkewSeconds;

    // map: keyId -> secret
    @Value("#{${skadi.peerCache.auth.sharedSecrets:{}}}")
    private Map<String, String> sharedSecrets;

    // Optional replay protection
    private final ConcurrentHashMap<String, Long> nonceSeen = new ConcurrentHashMap<>();

    public void verify(String method, String path, String query,
                       String keyId, String ts, String nonce, String sigB64Url) {

        if (sharedSecrets == null || sharedSecrets.isEmpty()) {
            // If auth is not configured, deny by default (safer)
            throw new Unauthorized();
        }

        if (keyId == null || ts == null || nonce == null || sigB64Url == null) throw new Unauthorized();

        long now = Instant.now().toEpochMilli();
        long t;
        try { t = Long.parseLong(ts); } catch (Exception e) { throw new Unauthorized(); }

        long skewMs = allowedSkewSeconds * 1000L;
        if (Math.abs(now - t) > skewMs) throw new Unauthorized();

        Long prev = nonceSeen.putIfAbsent(keyId + ":" + nonce, now);
        if (prev != null) throw new Unauthorized();

        String secret = sharedSecrets.get(keyId);
        if (secret == null || secret.isBlank()) throw new Unauthorized();

        String canonical = method + "\n" + path + "\n" + query + "\n" + ts + "\n" + nonce;
        String expected = hmacB64Url(secret, canonical);
        if (!constantTimeEquals(expected, sigB64Url)) throw new Unauthorized();
    }

    private static String hmacB64Url(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] out = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        if (x.length != y.length) return false;
        int r = 0;
        for (int i = 0; i < x.length; i++) r |= x[i] ^ y[i];
        return r == 0;
    }

    public static class Unauthorized extends RuntimeException {}
}
