package org.iceforge.skadi.semantic;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for the Lane D semantic contract metadata feature.
 *
 * <pre>{@code
 * skadi:
 *   semantic:
 *     contracts:
 *       enabled: true
 *       locations:
 *         - /etc/skadi/contracts/mxl_risk.json
 *         - /etc/skadi/contracts/credit.json
 * }</pre>
 *
 * <p>When {@code enabled} is {@code false} (the default), the contract registry
 * is empty and the metadata endpoint returns an empty contract list. Normal server
 * startup is not affected.
 */
@ConfigurationProperties(prefix = "skadi.semantic.contracts")
public class SemanticContractProperties {

    /** Whether the semantic contract feature is active. Defaults to {@code false}. */
    private boolean enabled = false;

    /**
     * Ordered list of filesystem paths to JSON contract files.
     * Each path must point to a file deserializable as
     * {@link org.iceforge.skadi.semantic.contract.SemanticContract}.
     * Ignored when {@code enabled} is {@code false}.
     */
    private List<String> locations = new ArrayList<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations != null ? locations : new ArrayList<>();
    }
}
