package org.iceforge.skadi.semantic.cache;

import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Structural and validation tests for cache boundary records.
 * No Spring context, no external services, no JSON serialization.
 */
class CacheBoundaryTest {

    static final String SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";
    static final CacheIdentity IDENTITY = new CacheIdentity(SQL, "alice", null);

    // ── CacheContract — factories ─────────────────────────────────────────────

    @Test
    void contract_none_hasNoneStrategy() {
        var c = CacheContract.none();
        assertEquals(SemanticCacheStrategy.NONE, c.strategy());
        assertNull(c.ttlSeconds());
    }

    @Test
    void contract_ttl_holdsTtl() {
        var c = CacheContract.ttl(300L);
        assertEquals(SemanticCacheStrategy.TTL, c.strategy());
        assertEquals(300L, c.ttlSeconds());
    }

    @Test
    void contract_ttl_rejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> CacheContract.ttl(0L));
    }

    @Test
    void contract_ttl_rejectsNullTtlSeconds() {
        assertThrows(IllegalArgumentException.class,
                () -> new CacheContract(SemanticCacheStrategy.TTL, null));
    }

    @Test
    void contract_datasetVersion_allowsNullTtl() {
        assertDoesNotThrow(() ->
                new CacheContract(SemanticCacheStrategy.DATASET_VERSION, null));
    }

    // ── CacheContract.from(SemanticCachePolicy) ───────────────────────────────

    @Test
    void contract_from_nonePolicy() {
        var c = CacheContract.from(SemanticCachePolicy.none());
        assertEquals(SemanticCacheStrategy.NONE, c.strategy());
        assertNull(c.ttlSeconds());
    }

    @Test
    void contract_from_ttlPolicy() {
        var c = CacheContract.from(SemanticCachePolicy.ttl(600L));
        assertEquals(SemanticCacheStrategy.TTL, c.strategy());
        assertEquals(600L, c.ttlSeconds());
    }

    @Test
    void contract_from_rejectsNull() {
        assertThrows(NullPointerException.class, () -> CacheContract.from(null));
    }

    // ── CachedArtifactMeta ────────────────────────────────────────────────────

    @Test
    void artifactMeta_holdsFields() {
        var now    = Instant.now();
        var expiry = now.plusSeconds(300);
        var meta   = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.LOCAL, 4096L, now, expiry);
        assertEquals(IDENTITY,                    meta.identity());
        assertEquals(CacheStorageBackend.LOCAL,   meta.storageBackend());
        assertEquals(4096L,                       meta.sizeBytes());
        assertEquals(now,                         meta.cachedAt());
        assertEquals(expiry,                      meta.expiresAt());
    }

    @Test
    void artifactMeta_nullExpiresAt_isValid() {
        var meta = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.S3, 0L, Instant.now(), null);
        assertNull(meta.expiresAt());
    }

    @Test
    void artifactMeta_zeroSizeBytes_isValid() {
        assertDoesNotThrow(() ->
                new CachedArtifactMeta(IDENTITY, CacheStorageBackend.LOCAL, 0L, Instant.now(), null));
    }

    @Test
    void artifactMeta_rejectsNegativeSizeBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new CachedArtifactMeta(IDENTITY, CacheStorageBackend.LOCAL, -1L, Instant.now(), null));
    }

    @Test
    void artifactMeta_rejectsNullIdentity() {
        assertThrows(NullPointerException.class,
                () -> new CachedArtifactMeta(null, CacheStorageBackend.LOCAL, 0L, Instant.now(), null));
    }

    @Test
    void artifactMeta_allBackends() {
        for (var backend : CacheStorageBackend.values()) {
            var meta = new CachedArtifactMeta(IDENTITY, backend, 0L, Instant.now(), null);
            assertEquals(backend, meta.storageBackend());
        }
    }

    // ── CacheLookupRequest ────────────────────────────────────────────────────

    @Test
    void lookupRequest_holdsFields() {
        var contract = CacheContract.ttl(300L);
        var req      = new CacheLookupRequest(IDENTITY, contract);
        assertEquals(IDENTITY, req.identity());
        assertEquals(contract, req.contract());
    }

    @Test
    void lookupRequest_rejectsNullIdentity() {
        assertThrows(NullPointerException.class,
                () -> new CacheLookupRequest(null, CacheContract.none()));
    }

    // ── CacheLookupResult ─────────────────────────────────────────────────────

    @Test
    void lookupResult_absent_hasNullMeta() {
        var r = CacheLookupResult.absent();
        assertEquals(CacheEntryState.ABSENT, r.state());
        assertNull(r.artifactMeta());
    }

    @Test
    void lookupResult_hit_hasFreshStateAndMeta() {
        var meta = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.LOCAL, 0L, Instant.now(), null);
        var r    = CacheLookupResult.hit(meta);
        assertEquals(CacheEntryState.FRESH, r.state());
        assertSame(meta, r.artifactMeta());
    }

    @Test
    void lookupResult_stale_hasStaleStateAndMeta() {
        var meta = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.S3, 0L, Instant.now(), null);
        var r    = CacheLookupResult.stale(meta);
        assertEquals(CacheEntryState.STALE, r.state());
        assertNotNull(r.artifactMeta());
    }

    @Test
    void lookupResult_invalidated_hasInvalidatedState() {
        var meta = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.S3, 0L, Instant.now(), null);
        var r    = CacheLookupResult.invalidated(meta);
        assertEquals(CacheEntryState.INVALIDATED, r.state());
    }

    @Test
    void lookupResult_nonAbsentState_requiresMeta() {
        assertThrows(IllegalArgumentException.class,
                () -> new CacheLookupResult(CacheEntryState.FRESH, null));
        assertThrows(IllegalArgumentException.class,
                () -> new CacheLookupResult(CacheEntryState.STALE, null));
        assertThrows(IllegalArgumentException.class,
                () -> new CacheLookupResult(CacheEntryState.INVALIDATED, null));
    }

    @Test
    void lookupResult_hit_rejectsNullMeta() {
        assertThrows(NullPointerException.class, () -> CacheLookupResult.hit(null));
    }

    // ── CacheWriteRequest ─────────────────────────────────────────────────────

    @Test
    void writeRequest_holdsFields() {
        var req = new CacheWriteRequest(IDENTITY, CacheContract.ttl(300L), 8192L);
        assertEquals(IDENTITY, req.identity());
        assertEquals(300L,     req.contract().ttlSeconds());
        assertEquals(8192L,    req.estimatedSizeBytes());
    }

    @Test
    void writeRequest_zeroSizeBytes_isValid() {
        assertDoesNotThrow(() -> new CacheWriteRequest(IDENTITY, CacheContract.none(), 0L));
    }

    @Test
    void writeRequest_rejectsNegativeSizeBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new CacheWriteRequest(IDENTITY, CacheContract.none(), -1L));
    }

    @Test
    void writeRequest_rejectsNullIdentity() {
        assertThrows(NullPointerException.class,
                () -> new CacheWriteRequest(null, CacheContract.none(), 0L));
    }

    // ── CacheStorageBackend / CacheEntryState — enum completeness ─────────────

    @Test
    void cacheStorageBackend_hasExpectedValues() {
        var values = CacheStorageBackend.values();
        assertEquals(3, values.length);
    }

    @Test
    void cacheEntryState_hasExpectedValues() {
        var values = CacheEntryState.values();
        assertEquals(4, values.length);
    }
}
