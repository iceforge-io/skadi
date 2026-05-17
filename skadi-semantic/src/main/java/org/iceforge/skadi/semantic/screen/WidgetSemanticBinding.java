package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

/**
 * The complete semantic binding for one widget — which contract, measures,
 * dimensions, and filters are currently active.
 *
 * <p>The buddy chat uses this binding to answer "What is this chart showing?"
 * by interrogating the governing semantic contract for the bound measures and
 * dimensions, and describing the applied filters in business terms.
 *
 * <p>{@link #filters()} and {@link #userEntitlementScope()} are optional; a widget
 * with no active filters or no scoped entitlement passes empty list / null respectively.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WidgetSemanticBinding(
        String widgetId,
        ContractBinding contractBinding,
        List<BoundMeasure> measures,
        List<BoundDimension> dimensions,
        List<AppliedFilter> filters,
        UserEntitlementScope userEntitlementScope) {

    /**
     * @param widgetId              stable widget identifier matching {@link ActiveWidgetContext#widgetId()}; must not be null or blank
     * @param contractBinding       the contract and version powering this widget; must not be null
     * @param measures              measures displayed in this widget; must not be null; defensively copied
     * @param dimensions            dimensions used for grouping; must not be null; defensively copied
     * @param filters               active filter predicates; must not be null; defensively copied
     * @param userEntitlementScope  entitlement scope of the viewing user; may be null
     */
    public WidgetSemanticBinding {
        Objects.requireNonNull(widgetId,         "widgetId must not be null");
        Objects.requireNonNull(contractBinding,  "contractBinding must not be null");
        Objects.requireNonNull(measures,         "measures must not be null");
        Objects.requireNonNull(dimensions,       "dimensions must not be null");
        Objects.requireNonNull(filters,          "filters must not be null");
        if (widgetId.isBlank()) throw new IllegalArgumentException("widgetId must not be blank");
        measures   = List.copyOf(measures);
        dimensions = List.copyOf(dimensions);
        filters    = List.copyOf(filters);
    }
}
