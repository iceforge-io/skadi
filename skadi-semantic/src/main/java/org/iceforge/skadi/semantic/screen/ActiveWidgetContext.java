package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * Identifies the widget currently focused or active on the dashboard screen.
 *
 * <p>The buddy chat uses this to determine which widget the user is asking about
 * when they ask "What is this chart showing?" without naming a specific widget.
 *
 * <p>The {@link #widgetType()} describes the visualisation type (e.g.
 * {@code "bar_chart"}, {@code "line_chart"}, {@code "table"}, {@code "metric_card"})
 * and helps the buddy chat tailor its explanation to the visual form.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ActiveWidgetContext(
        String widgetId,
        String widgetType) {

    /**
     * @param widgetId   stable widget identifier matching {@link WidgetSemanticBinding#widgetId()}; must not be null or blank
     * @param widgetType visualisation type string; must not be null or blank
     */
    public ActiveWidgetContext {
        Objects.requireNonNull(widgetId,   "widgetId must not be null");
        Objects.requireNonNull(widgetType, "widgetType must not be null");
        if (widgetId.isBlank())   throw new IllegalArgumentException("widgetId must not be blank");
        if (widgetType.isBlank()) throw new IllegalArgumentException("widgetType must not be blank");
    }
}
