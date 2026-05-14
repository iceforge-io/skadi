package org.iceforge.skadi.semantic.cache;

import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;

import java.util.Objects;

/**
 * A cache policy hint passed from the semantic layer to the cache layer.
 *
 * <p>A {@code CacheContract} carries the minimum information the cache layer
 * needs to honour a policy decision: the {@link SemanticCacheStrategy strategy}
 * and an optional TTL. It is a boundary type — it does not perform cache I/O and
 * does not carry the full governance metadata of a
 * {@link SemanticCachePolicy} (rule refs, etc. are stripped at the boundary).
 *
 * <p>The cache layer ({@code skadi-server}) may accept or override these hints
 * according to its own configuration; this record declares a preference, not
 * a mandate.
 *
 * <p>{@link #ttlSeconds()} is {@code null} for non-TTL strategies.
 * {@link SemanticCacheStrategy#DATASET_VERSION} is declared but has no runtime
 * effect in Lane C (gap L3, DQR-001).
 *
 * <pre>{@code
 * var noCache = CacheContract.none();
 * var fiveMin = CacheContract.ttl(300L);
 * var fromPolicy = CacheContract.from(SemanticCachePolicy.ttl(600L));
 * }</pre>
 */
public record CacheContract(
        SemanticCacheStrategy strategy,
        Long ttlSeconds) {

    /**
     * @param strategy   cache strategy; must not be null
     * @param ttlSeconds TTL in seconds; must be positive when strategy is
     *                   {@link SemanticCacheStrategy#TTL}, otherwise null
     */
    public CacheContract {
        Objects.requireNonNull(strategy, "strategy must not be null");
        if (strategy == SemanticCacheStrategy.TTL) {
            if (ttlSeconds == null) {
                throw new IllegalArgumentException("ttlSeconds must not be null when strategy is TTL");
            }
            if (ttlSeconds <= 0) {
                throw new IllegalArgumentException("ttlSeconds must be positive, was: " + ttlSeconds);
            }
        }
    }

    /** Returns a contract declaring that results should not be cached. */
    public static CacheContract none() {
        return new CacheContract(SemanticCacheStrategy.NONE, null);
    }

    /**
     * Returns a TTL-based cache contract.
     *
     * @param ttlSeconds positive TTL in seconds
     */
    public static CacheContract ttl(long ttlSeconds) {
        return new CacheContract(SemanticCacheStrategy.TTL, ttlSeconds);
    }

    /**
     * Converts a {@link SemanticCachePolicy} from the semantic contract vocabulary
     * into a {@code CacheContract} for the cache boundary.
     *
     * <p>Governance fields (rule refs) are intentionally not propagated —
     * the cache layer does not need governance metadata.
     *
     * @param policy source policy; must not be null
     */
    public static CacheContract from(SemanticCachePolicy policy) {
        Objects.requireNonNull(policy, "policy must not be null");
        return new CacheContract(policy.strategy(), policy.ttlSeconds());
    }
}
