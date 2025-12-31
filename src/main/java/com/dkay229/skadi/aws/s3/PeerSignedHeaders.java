package com.dkay229.skadi.aws.s3;

import org.springframework.http.HttpHeaders;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

public record PeerSignedHeaders(String keyId, String ts, String nonce, String signature) {

    public void apply(HttpHeaders h) {
        h.set("X-Skadi-KeyId", keyId);
        h.set("X-Skadi-Ts", ts);
        h.set("X-Skadi-Nonce", nonce);
        h.set("X-Skadi-Signature", signature);
    }

    public static PeerSignedHeaders sign(String method, String path, String query, String keyId, String secret) {
        String ts = String.valueOf(Instant.now().toEpochMilli());
        String nonce = UUID.randomUUID().toString();
        String canonical = method + "\n" + path + "\n" + query + "\n" + ts + "\n" + nonce;
        String sig = hmacB64Url(secret, canonical);
        return new PeerSignedHeaders(keyId, ts, nonce, sig);
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
}
