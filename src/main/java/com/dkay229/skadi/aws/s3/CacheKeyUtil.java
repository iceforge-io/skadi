package com.dkay229.skadi.aws.s3;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class CacheKeyUtil {
    private CacheKeyUtil() {}

    public static String cacheId(String bucket, String key) {
        return sha256Hex(bucket + "\n" + key);
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 unavailable", e);
        }
    }
}
