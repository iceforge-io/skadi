package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * A plain-English query request for the market-risk sensitivity demo.
 *
 * <p>This is the raw inbound DTO — the caller provides free-form {@link #text()}
 * and an optional {@link #limit()}. The text is passed to the (future)
 * plain-English interpreter which produces a {@link DemoSemanticQueryInterpretation}.
 *
 * <p>No SQL, no contract IDs, and no measure names appear here; those are
 * resolved by the interpreter, not the caller.
 *
 * <pre>{@code
 * new DemoSemanticQueryRequest(
 *     "Show me delta exposure by legal entity and risk class for CIB in USD as of 2026-05-15",
 *     null);
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticQueryRequest(
        String text,
        Integer limit) {

    /**
     * @param text  plain-English query text; must not be null or blank
     * @param limit optional row limit; null means no limit
     */
    public DemoSemanticQueryRequest {
        Objects.requireNonNull(text, "text must not be null");
        if (text.isBlank()) throw new IllegalArgumentException("text must not be blank");
        if (limit != null && limit <= 0) {
            throw new IllegalArgumentException("limit must be positive when set, was: " + limit);
        }
    }

    /** Convenience constructor with no limit. */
    public DemoSemanticQueryRequest(String text) {
        this(text, null);
    }
}
