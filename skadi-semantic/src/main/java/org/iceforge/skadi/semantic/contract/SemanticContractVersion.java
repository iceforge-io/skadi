package org.iceforge.skadi.semantic.contract;

import java.util.Objects;

/**
 * Versioning metadata for a {@link SemanticContract}.
 *
 * <p>The version string follows semantic versioning (semver). It identifies
 * the version of the contract definition itself — not the version of the
 * underlying physical data.
 *
 * <p>Breaking changes to a contract (renaming a measure, removing a dimension)
 * require a major version bump and a migration note.
 *
 * <pre>{@code
 * var v = new SemanticContractVersion("1.0.0");
 * }</pre>
 */
public record SemanticContractVersion(String value) {

    /**
     * @param value semver string, e.g. {@code "1.0.0"}; must not be null or blank
     */
    public SemanticContractVersion {
        Objects.requireNonNull(value, "version value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("version value must not be blank");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
