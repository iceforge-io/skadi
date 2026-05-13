package org.iceforge.skadi.sqlgateway.security;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SecretRedactorTest {

    @Test
    void safe_keys_pass_through_unchanged() {
        Map<String, String> params = Map.of(
                "user", "alice",
                "database", "postgres",
                "application_name", "Tableau Desktop"
        );
        Map<String, String> safe = SecretRedactor.redact(params);
        assertThat(safe).containsEntry("user", "alice");
        assertThat(safe).containsEntry("database", "postgres");
        assertThat(safe).containsEntry("application_name", "Tableau Desktop");
    }

    @Test
    void password_key_is_redacted() {
        assertThat(SecretRedactor.isSensitiveKey("password")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("PASSWORD")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("db_password")).isTrue();
    }

    @Test
    void token_key_is_redacted() {
        assertThat(SecretRedactor.isSensitiveKey("token")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("access_token")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("TOKEN")).isTrue();
    }

    @Test
    void secret_key_is_redacted() {
        assertThat(SecretRedactor.isSensitiveKey("secret")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("client_secret")).isTrue();
    }

    @Test
    void credential_and_auth_keys_are_redacted() {
        assertThat(SecretRedactor.isSensitiveKey("credential")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("authorization")).isTrue();
        assertThat(SecretRedactor.isSensitiveKey("auth_header")).isTrue();
    }

    @Test
    void redact_replaces_sensitive_values_with_stars() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("user", "alice");
        params.put("password", "s3cr3t!");
        params.put("token", "tok_abc123");
        params.put("database", "prod");

        Map<String, String> safe = SecretRedactor.redact(params);

        assertThat(safe.get("user")).isEqualTo("alice");
        assertThat(safe.get("password")).isEqualTo("***");
        assertThat(safe.get("token")).isEqualTo("***");
        assertThat(safe.get("database")).isEqualTo("prod");
    }

    @Test
    void null_map_returns_empty() {
        assertThat(SecretRedactor.redact(null)).isEmpty();
    }

    @Test
    void empty_map_returns_empty() {
        assertThat(SecretRedactor.redact(Map.of())).isEmpty();
    }

    @Test
    void null_key_is_treated_as_not_sensitive() {
        assertThat(SecretRedactor.isSensitiveKey(null)).isFalse();
    }

    @Test
    void safe_key_with_partial_match_not_redacted() {
        // "user" contains no sensitive keyword; "key" does
        assertThat(SecretRedactor.isSensitiveKey("user")).isFalse();
        assertThat(SecretRedactor.isSensitiveKey("api_key")).isTrue();
    }

    @Test
    void input_map_is_not_modified() {
        Map<String, String> original = new LinkedHashMap<>();
        original.put("password", "secret");
        original.put("user", "bob");

        SecretRedactor.redact(original);

        assertThat(original.get("password")).isEqualTo("secret"); // unchanged
    }
}
