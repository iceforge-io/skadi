package org.iceforge.skadi.semantic.query;

/**
 * Display-only formatting hints for a {@link SemanticOutputColumn}.
 *
 * <p>A {@code SemanticFormatHint} provides rendering guidance to consumers such as
 * UI bricks, spreadsheet exports, and report formatters. All fields are optional
 * (nullable) — consumers must handle null gracefully and fall back to default
 * formatting if a hint is absent.
 *
 * <p>This record is a descriptor only — it does not perform any formatting or
 * rendering itself.
 *
 * <p>Typical usage:
 * <pre>{@code
 * // A monetary PnL column
 * var hint = new SemanticFormatHint("#,##0.00", "GBP", "GBP", 2, 2);
 *
 * // A percentage column
 * var pct  = new SemanticFormatHint("0.00%", null, null, 2, null);
 *
 * // A date column
 * var date = new SemanticFormatHint("yyyy-MM-dd", null, null, null, null);
 * }</pre>
 */
public record SemanticFormatHint(
        String  pattern,
        String  unit,
        String  currency,
        Integer precision,
        Integer scale) {

    // No null checks — every field is intentionally optional.
    // Consumers must handle null gracefully.
}
