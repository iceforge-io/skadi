package org.iceforge.skadi.semantic.contract;

/**
 * Cache invalidation strategy declared in a {@link SemanticCachePolicy}.
 *
 * <p>This enum is a descriptor only — it declares the intended caching
 * behaviour for a contract. No cache lookup, write, or eviction logic exists
 * in this type. The cache layer ({@code skadi-server}) interprets these hints;
 * see {@code ai/adr/ADR-010-cache-positioning.md}.
 *
 * <p>{@link #DATASET_VERSION} is declared here as a future extension seam.
 * The version resolver required to implement it is out of scope for Lane C
 * (tracked as gap L3 and DQR-001).
 */
public enum SemanticCacheStrategy {

    /** Do not cache results for this contract. */
    NONE,

    /** Cache results for a fixed time-to-live ({@link SemanticCachePolicy#ttlSeconds()}). */
    TTL,

    /**
     * Cache results keyed by dataset version token.
     *
     * <p><strong>Not implemented in Lane C.</strong> The version resolver is a
     * post-Lane C concern (gap L3). A {@link SemanticCachePolicy} using this
     * strategy will be treated as {@link #NONE} until the resolver is activated.
     */
    DATASET_VERSION
}
