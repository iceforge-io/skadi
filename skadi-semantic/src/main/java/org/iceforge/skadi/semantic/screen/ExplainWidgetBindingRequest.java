package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * A request for the buddy chat to explain what a specific widget is showing.
 *
 * <p>This is the primary input type for ADR-012 screen-context-aware explanations.
 * The caller provides the target widget ID and the full dashboard screen context;
 * the buddy chat resolves the widget's semantic binding and interrogates the
 * governing contract to produce a governed explanation.
 *
 * <p>This record defines the input contract only — buddy-chat runtime and LLM
 * integration are out of scope for Lane E1.
 *
 * <pre>{@code
 * var request = new ExplainWidgetBindingRequest(
 *     "widget-pnl-by-book",
 *     new DashboardScreenContext(
 *         "dashboard-risk-overview",
 *         new ActiveWidgetContext("widget-pnl-by-book", "bar_chart"),
 *         List.of(binding)));
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ExplainWidgetBindingRequest(
        String widgetId,
        DashboardScreenContext screenContext) {

    /**
     * @param widgetId      the ID of the widget to explain; must not be null or blank
     * @param screenContext the full dashboard screen context; must not be null
     */
    public ExplainWidgetBindingRequest {
        Objects.requireNonNull(widgetId,      "widgetId must not be null");
        Objects.requireNonNull(screenContext, "screenContext must not be null");
        if (widgetId.isBlank()) throw new IllegalArgumentException("widgetId must not be blank");
    }
}
