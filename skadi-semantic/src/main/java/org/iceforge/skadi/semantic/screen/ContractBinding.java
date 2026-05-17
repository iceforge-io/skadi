package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * Identifies which semantic contract and version powers a widget.
 *
 * <p>The {@link #contractId()} is the lookup key in the {@code ContractRegistry}.
 * The {@link #contractVersion()} records which contract version was active when
 * the widget was rendered, enabling the buddy chat to detect stale bindings.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ContractBinding(
        String contractId,
        String contractVersion) {

    /**
     * @param contractId      name of the semantic contract; must not be null or blank
     * @param contractVersion semver string of the contract in use; must not be null or blank
     */
    public ContractBinding {
        Objects.requireNonNull(contractId,      "contractId must not be null");
        Objects.requireNonNull(contractVersion, "contractVersion must not be null");
        if (contractId.isBlank())      throw new IllegalArgumentException("contractId must not be blank");
        if (contractVersion.isBlank()) throw new IllegalArgumentException("contractVersion must not be blank");
    }
}
