package org.iceforge.skadi.semantic.contract;

import java.util.List;
import java.util.Objects;

/**
 * Descriptive cache policy hint for a {@link SemanticContract}.
 *
 * <p>A {@code SemanticCachePolicy} declares the intended caching behaviour for
 * a contract's query results. It is a hint to the cache layer — it does not
 * perform cache lookups, writes, or evictions. The cache layer ({@code skadi-server})
 * may accept or override these hints according to its own configuration;
 * see {@code ai/adr/ADR-010-cache-positioning.md}.
 *
 * <p>{@link #ttlSeconds()} is {@code null} when the strategy is {@link SemanticCacheStrategy#NONE}
 * or {@link SemanticCacheStrategy#DATASET_VERSION}. It must be a positive value
 * when the strategy is {@link SemanticCacheStrategy#TTL}.
 *
 * <p>{@link SemanticCacheStrategy#DATASET_VERSION} is reserved as a future extension
 * seam (gap L3, DQR-001) and has no effect in Lane C.
 *
 * <p>Static factories for the common cases:
 * <pre>{@code
 * var noCache  = SemanticCachePolicy.none();
 * var fiveMin  = SemanticCachePolicy.ttl(300L);
 * }</pre>
 */
public record SemanticCachePolicy(
        SemanticCacheStrategy strategy,
        Long ttlSeconds,
        List<SemanticRuleRef> ruleRefs) {

    /**
     * @param strategy   cache strategy; must not be null
     * @param ttlSeconds TTL in seconds; required (positive) when strategy is
     *                   {@link SemanticCacheStrategy#TTL}, otherwise {@code null}
     * @param ruleRefs   governance rule references that constrain caching;
     *                   must not be null; copied defensively
     */
    public SemanticCachePolicy {
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(ruleRefs, "ruleRefs must not be null");
        if (strategy == SemanticCacheStrategy.TTL) {
            if (ttlSeconds == null) {
                throw new IllegalArgumentException("ttlSeconds must not be null when strategy is TTL");
            }
            if (ttlSeconds <= 0) {
                throw new IllegalArgumentException("ttlSeconds must be positive, was: " + ttlSeconds);
            }
        }
        ruleRefs = List.copyOf(ruleRefs);
    }

    /**
     * Returns a policy declaring that results for this contract should not be cached.
     */
    public static SemanticCachePolicy none() {
        return new SemanticCachePolicy(SemanticCacheStrategy.NONE, null, List.of());
    }

    /**
     * Returns a TTL-based cache policy with the given time-to-live.
     *
     * @param ttlSeconds positive TTL in seconds
     */
    public static SemanticCachePolicy ttl(long ttlSeconds) {
        return new SemanticCachePolicy(SemanticCacheStrategy.TTL, ttlSeconds, List.of());
    }
}
