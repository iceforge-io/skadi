package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The complete semantic context of the dashboard screen at the moment the user
 * sends a buddy-chat question.
 *
 * <p>The buddy chat uses this object to answer screen-aware questions like
 * "What is this chart showing?" by correlating {@link #activeWidget()} with the
 * matching entry in {@link #widgetBindings()} and interrogating the governing
 * semantic contract.
 *
 * <p>{@link #dashboardId()} identifies which dashboard is open (nullable — the
 * dashboard identity may not be available in all contexts). {@link #activeWidget()}
 * is nullable when no widget has focus. {@link #widgetBindings()} is never null
 * but may be empty when no semantic widgets are on screen.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DashboardScreenContext(
        String dashboardId,
        ActiveWidgetContext activeWidget,
        List<WidgetSemanticBinding> widgetBindings) {

    /**
     * @param dashboardId    identifier of the open dashboard; may be null
     * @param activeWidget   the currently focused widget; may be null
     * @param widgetBindings semantic bindings for all widgets on the screen;
     *                       must not be null; defensively copied
     */
    public DashboardScreenContext {
        Objects.requireNonNull(widgetBindings, "widgetBindings must not be null");
        widgetBindings = List.copyOf(widgetBindings);
    }

    /**
     * Returns the binding for the active widget, if one is active and its binding is present.
     */
    public Optional<WidgetSemanticBinding> activeWidgetBinding() {
        if (activeWidget == null) return Optional.empty();
        return widgetBindings.stream()
                .filter(b -> b.widgetId().equals(activeWidget.widgetId()))
                .findFirst();
    }
}
