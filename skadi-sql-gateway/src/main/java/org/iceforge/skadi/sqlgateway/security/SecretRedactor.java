package org.iceforge.skadi.sqlgateway.security;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Centralised utility for redacting sensitive values from parameter maps and strings.
 *
 * <p>Any map key whose lower-cased form contains one of the {@link #SENSITIVE_KEYWORDS}
 * has its value replaced with {@code "***"}.  All other keys are passed through unchanged.
 *
 * <p>Used in {@code TableauTraceLogger} (startup params) and any other log/audit path
 * that could receive user-supplied key-value pairs.
 */
public final class SecretRedactor {

    static final Set<String> SENSITIVE_KEYWORDS = Set.of(
            "password", "passwd", "pwd",
            "token", "secret", "credential",
            "key", "auth", "authorization"
    );

    static final String REDACTED = "***";

    private SecretRedactor() {}

    /**
     * Returns a new map with sensitive values replaced by {@value #REDACTED}.
     * The input map is never modified; insertion order is preserved.
     */
    public static Map<String, String> redact(Map<String, String> params) {
        if (params == null || params.isEmpty()) return Map.of();
        Map<String, String> safe = new LinkedHashMap<>(params.size());
        params.forEach((k, v) -> safe.put(k, isSensitiveKey(k) ? REDACTED : v));
        return safe;
    }

    /** Returns {@code true} when the key name suggests it holds a secret value. */
    public static boolean isSensitiveKey(String key) {
        if (key == null) return false;
        String lower = key.toLowerCase(java.util.Locale.ROOT);
        for (String kw : SENSITIVE_KEYWORDS) {
            if (lower.contains(kw)) return true;
        }
        return false;
    }
}
