package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

/**
 * A single filter criterion in a {@link DemoSemanticQueryInterpretation}.
 *
 * <p>Supports three filter forms:
 * <ul>
 *   <li><b>Equality</b> — {@link #name()} + {@link #value()}, operator null or "EQ"</li>
 *   <li><b>IN-style</b> — {@link #name()} + {@link #values()} list, operator "IN"</li>
 *   <li><b>Date range</b> — {@link #name()} + {@link #start()} / {@link #end()},
 *       operator "BETWEEN"; date math strings (e.g. "last_30_days") are also
 *       accepted in {@link #value()} and resolved by the future renderer</li>
 * </ul>
 *
 * <p>Unused optional fields are null; Jackson skips them via {@link JsonInclude}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticFilter(
        String name,
        String value,
        String operator,
        List<String> values,
        String start,
        String end) {

    /**
     * @param name     filter dimension name; must not be null or blank
     * @param value    simple equality value; null for IN or range filters
     * @param operator optional operator string (e.g. "EQ", "IN", "BETWEEN"); null defaults to EQ
     * @param values   value list for IN-style filters; null otherwise; copied defensively
     * @param start    range start (date string or token); null if not a range filter
     * @param end      range end (date string or token); null if not a range filter
     */
    public DemoSemanticFilter {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
        values = values == null ? null : List.copyOf(values);
    }

    /** Convenience constructor for a simple equality filter. */
    public static DemoSemanticFilter eq(String name, String value) {
        Objects.requireNonNull(value, "value must not be null for equality filter");
        return new DemoSemanticFilter(name, value, null, null, null, null);
    }

    /** Convenience constructor for an IN-style filter. */
    public static DemoSemanticFilter in(String name, List<String> values) {
        Objects.requireNonNull(values, "values must not be null for IN filter");
        return new DemoSemanticFilter(name, null, "IN", values, null, null);
    }

    /** Convenience constructor for a date-range filter. */
    public static DemoSemanticFilter between(String name, String start, String end) {
        return new DemoSemanticFilter(name, null, "BETWEEN", null, start, end);
    }
}
