package org.iceforge.skadi.semantic;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the market-risk sensitivity plain-English demo spike.
 *
 * <pre>{@code
 * skadi:
 *   semantic:
 *     demo:
 *       enabled: false                                              # default; must opt-in
 *       default-limit: 500
 *       max-limit: 5000
 *       exposure-view: demo.market_risk.v_sensitivity_exposure_demo
 *       history-view:  demo.market_risk.v_sensitivity_history_demo
 * }</pre>
 *
 * <p>The endpoint is inactive when {@code enabled=false} (the default).
 * Set {@code enabled=true} explicitly to activate the demo spike endpoint.
 */
@ConfigurationProperties(prefix = "skadi.semantic.demo")
public class DemoSemanticProperties {

    /** Whether the demo endpoint is active. Defaults to {@code false}. */
    private boolean enabled = false;

    /** Default row limit applied to rendered SQL. */
    private int defaultLimit = 500;

    /** Hard maximum cap applied to rendered SQL row limit. */
    private int maxLimit = 5000;

    /** Fully-qualified view name for exposure-grid (GRID) queries. */
    private String exposureView = "demo.market_risk.v_sensitivity_exposure_demo";

    /** Fully-qualified view name for historical time-series (TIMESERIES) queries. */
    private String historyView = "demo.market_risk.v_sensitivity_history_demo";

    public boolean isEnabled()           { return enabled; }
    public void setEnabled(boolean v)    { this.enabled = v; }

    public int getDefaultLimit()         { return defaultLimit; }
    public void setDefaultLimit(int v)   { this.defaultLimit = v; }

    public int getMaxLimit()             { return maxLimit; }
    public void setMaxLimit(int v)       { this.maxLimit = v; }

    public String getExposureView()           { return exposureView; }
    public void   setExposureView(String v)   { this.exposureView = v; }

    public String getHistoryView()            { return historyView; }
    public void   setHistoryView(String v)    { this.historyView = v; }
}
