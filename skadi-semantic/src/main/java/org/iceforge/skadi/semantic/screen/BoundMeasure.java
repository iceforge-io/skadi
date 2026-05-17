package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * A measure currently selected and displayed within a widget.
 *
 * <p>The {@link #measureId()} must match a measure name defined in the widget's
 * governing semantic contract. The optional {@link #label()} is the display label
 * rendered in the widget's legend or axis.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BoundMeasure(
        String measureId,
        String label) {

    /**
     * @param measureId the measure name as defined in the semantic contract; must not be null or blank
     * @param label     optional display label for the measure; may be null
     */
    public BoundMeasure {
        Objects.requireNonNull(measureId, "measureId must not be null");
        if (measureId.isBlank()) throw new IllegalArgumentException("measureId must not be blank");
    }
}
