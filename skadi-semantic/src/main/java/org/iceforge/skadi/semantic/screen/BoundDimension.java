package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * A dimension used for grouping or breakdown in a widget.
 *
 * <p>The {@link #dimensionId()} must match a dimension name defined in the widget's
 * governing semantic contract and must have {@code groupable=true} in that contract.
 * The optional {@link #label()} is the display label rendered on the axis or in legends.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BoundDimension(
        String dimensionId,
        String label) {

    /**
     * @param dimensionId the dimension name as defined in the semantic contract; must not be null or blank
     * @param label       optional display label for the dimension; may be null
     */
    public BoundDimension {
        Objects.requireNonNull(dimensionId, "dimensionId must not be null");
        if (dimensionId.isBlank()) throw new IllegalArgumentException("dimensionId must not be blank");
    }
}
