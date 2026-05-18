package org.iceforge.skadi.semantic.demo;

import java.util.Objects;

/**
 * View-name configuration for the market-risk sensitivity demo SQL renderer.
 *
 * <p>Carries the fully-qualified names of the two demo views:
 * {@link #exposureView()} for GRID queries and {@link #historyView()} for
 * TIMESERIES queries. These names are operator-controlled configuration, not
 * user input, and are embedded directly into rendered SQL.
 *
 * <p>Default placeholders (safe, non-production):
 * <ul>
 *   <li>{@code demo.market_risk.v_sensitivity_exposure_demo}</li>
 *   <li>{@code demo.market_risk.v_sensitivity_history_demo}</li>
 * </ul>
 *
 * <pre>{@code
 * // Use placeholder defaults for the demo spike
 * DemoViewConfig config = DemoViewConfig.defaults();
 *
 * // Override for a specific environment
 * DemoViewConfig config = new DemoViewConfig(
 *     "dev.market_risk.v_exposure_demo",
 *     "dev.market_risk.v_history_demo");
 * }</pre>
 */
public record DemoViewConfig(String exposureView, String historyView) {

    private static final String DEFAULT_EXPOSURE_VIEW =
            "demo.market_risk.v_sensitivity_exposure_demo";
    private static final String DEFAULT_HISTORY_VIEW  =
            "demo.market_risk.v_sensitivity_history_demo";

    /**
     * @param exposureView fully-qualified view name for GRID queries; must not be null or blank
     * @param historyView  fully-qualified view name for TIMESERIES queries; must not be null or blank
     */
    public DemoViewConfig {
        Objects.requireNonNull(exposureView, "exposureView must not be null");
        Objects.requireNonNull(historyView,  "historyView must not be null");
        if (exposureView.isBlank()) throw new IllegalArgumentException("exposureView must not be blank");
        if (historyView.isBlank())  throw new IllegalArgumentException("historyView must not be blank");
    }

    /**
     * Returns the default demo view configuration using safe placeholder view names
     * that do not point to confidential production tables.
     */
    public static DemoViewConfig defaults() {
        return new DemoViewConfig(DEFAULT_EXPOSURE_VIEW, DEFAULT_HISTORY_VIEW);
    }
}
