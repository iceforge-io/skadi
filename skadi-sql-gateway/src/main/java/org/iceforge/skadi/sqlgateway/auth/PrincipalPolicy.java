package org.iceforge.skadi.sqlgateway.auth;

import java.util.List;
import java.util.Locale;

/**
 * Authorization policy for a single principal.
 *
 * <p>An unrestricted policy ({@code allowedSchemas == null}) permits access to all schemas.
 * A restricted policy only permits the listed schemas (case-insensitive match).
 */
public record PrincipalPolicy(List<String> allowedSchemas) {

    public static final PrincipalPolicy UNRESTRICTED = new PrincipalPolicy(null);

    public boolean isUnrestricted() {
        return allowedSchemas == null;
    }

    /** Returns true if the principal may access the given schema. */
    public boolean permitsSchema(String schema) {
        if (isUnrestricted()) return true;
        if (schema == null) return false;
        String lower = schema.toLowerCase(Locale.ROOT);
        return allowedSchemas.stream().anyMatch(s -> s.toLowerCase(Locale.ROOT).equals(lower));
    }
}
