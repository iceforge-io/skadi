package org.iceforge.skadi.semantic.demo;

/**
 * The requested output shape for a demo semantic query.
 *
 * <p>GRID requests a tabular cross-section (group-by across one or more risk
 * dimensions on a single COB date). TIMESERIES requests a date-ordered series
 * grouped by one or more slice keys.
 */
public enum DemoSemanticOutputShape {

    /** Tabular exposure grid — one row per grouping cell on a single COB date. */
    GRID,

    /** Date-ordered time series — one row per (cob_date, grouping key) combination. */
    TIMESERIES
}
