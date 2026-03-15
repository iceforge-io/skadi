package org.iceforge.skadi.sqlgateway.auth;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;

import java.util.Map;
import java.util.Optional;

/** Resolves the {@link PrincipalPolicy} for an authenticated user from config. */
public final class PrincipalPolicyRegistry {

    private final Map<String, SqlGatewayProperties.PgWire.Auth.PolicyConfig> policies;

    public PrincipalPolicyRegistry(SqlGatewayProperties.PgWire.Auth auth) {
        Map<String, SqlGatewayProperties.PgWire.Auth.PolicyConfig> p =
                (auth != null && auth.policies() != null) ? auth.policies() : Map.of();
        this.policies = p;
    }

    /**
     * Returns the policy for {@code user}, or {@link PrincipalPolicy#UNRESTRICTED} if no
     * policy is configured for that user.
     */
    public PrincipalPolicy policyFor(String user) {
        if (user == null || user.isBlank()) return PrincipalPolicy.UNRESTRICTED;
        SqlGatewayProperties.PgWire.Auth.PolicyConfig cfg = policies.get(user);
        if (cfg == null || cfg.allowedSchemas() == null || cfg.allowedSchemas().isEmpty()) {
            return PrincipalPolicy.UNRESTRICTED;
        }
        return new PrincipalPolicy(cfg.allowedSchemas());
    }
}
