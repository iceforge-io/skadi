package org.iceforge.skadi.semantic.cache;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JSON serialization/deserialization tests for cache boundary records.
 * No Spring context, no external services.
 */
class CacheBoundaryJsonTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    static final String SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";
    static final CacheIdentity IDENTITY = new CacheIdentity(SQL, "alice", null);

    // ── CacheIdentity ─────────────────────────────────────────────────────────

    @Test
    void roundTrip_CacheIdentity() throws Exception {
        var id  = new CacheIdentity(SQL, "alice", "snap-001");
        var id2 = mapper.readValue(mapper.writeValueAsString(id), CacheIdentity.class);
        assertEquals(id.normalizedSql(),        id2.normalizedSql());
        assertEquals(id.principalName(),        id2.principalName());
        assertEquals(id.datasetVersionToken(),  id2.datasetVersionToken());
    }

    @Test
    void roundTrip_CacheIdentity_nullVersionToken() throws Exception {
        var id  = IDENTITY;
        var id2 = mapper.readValue(mapper.writeValueAsString(id), CacheIdentity.class);
        assertNull(id2.datasetVersionToken());
    }

    @Test
    void roundTrip_CacheIdentity_preservesFingerprint() throws Exception {
        var id  = IDENTITY;
        var id2 = mapper.readValue(mapper.writeValueAsString(id), CacheIdentity.class);
        assertEquals(id.fingerprint(), id2.fingerprint());
    }

    // ── CacheContract ─────────────────────────────────────────────────────────

    @Test
    void roundTrip_CacheContract_none() throws Exception {
        var c  = CacheContract.none();
        var c2 = mapper.readValue(mapper.writeValueAsString(c), CacheContract.class);
        assertEquals(SemanticCacheStrategy.NONE, c2.strategy());
        assertNull(c2.ttlSeconds());
    }

    @Test
    void roundTrip_CacheContract_ttl() throws Exception {
        var c  = CacheContract.ttl(600L);
        var c2 = mapper.readValue(mapper.writeValueAsString(c), CacheContract.class);
        assertEquals(SemanticCacheStrategy.TTL, c2.strategy());
        assertEquals(600L, c2.ttlSeconds());
    }

    @Test
    void roundTrip_CacheContract_datasetVersion() throws Exception {
        var c  = new CacheContract(SemanticCacheStrategy.DATASET_VERSION, null);
        var c2 = mapper.readValue(mapper.writeValueAsString(c), CacheContract.class);
        assertEquals(SemanticCacheStrategy.DATASET_VERSION, c2.strategy());
        assertNull(c2.ttlSeconds());
    }

    // ── CachedArtifactMeta ────────────────────────────────────────────────────

    @Test
    void roundTrip_CachedArtifactMeta() throws Exception {
        var now    = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var expiry = now.plusSeconds(300);
        var meta   = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.LOCAL, 4096L, now, expiry);
        var meta2  = mapper.readValue(mapper.writeValueAsString(meta), CachedArtifactMeta.class);
        assertEquals(meta.identity().normalizedSql(), meta2.identity().normalizedSql());
        assertEquals(CacheStorageBackend.LOCAL,       meta2.storageBackend());
        assertEquals(4096L,                           meta2.sizeBytes());
        assertEquals(now,                             meta2.cachedAt());
        assertEquals(expiry,                          meta2.expiresAt());
    }

    @Test
    void roundTrip_CachedArtifactMeta_nullExpiry() throws Exception {
        var meta  = new CachedArtifactMeta(IDENTITY, CacheStorageBackend.S3, 0L, Instant.now().truncatedTo(ChronoUnit.MILLIS), null);
        var meta2 = mapper.readValue(mapper.writeValueAsString(meta), CachedArtifactMeta.class);
        assertNull(meta2.expiresAt());
        assertEquals(CacheStorageBackend.S3, meta2.storageBackend());
    }

    @Test
    void roundTrip_allStorageBackends() throws Exception {
        for (var backend : CacheStorageBackend.values()) {
            var meta  = new CachedArtifactMeta(IDENTITY, backend, 0L, Instant.now().truncatedTo(ChronoUnit.MILLIS), null);
            var meta2 = mapper.readValue(mapper.writeValueAsString(meta), CachedArtifactMeta.class);
            assertEquals(backend, meta2.storageBackend());
        }
    }

    // ── CacheLookupRequest ────────────────────────────────────────────────────

    @Test
    void roundTrip_CacheLookupRequest() throws Exception {
        var req  = new CacheLookupRequest(IDENTITY, CacheContract.ttl(300L));
        var req2 = mapper.readValue(mapper.writeValueAsString(req), CacheLookupRequest.class);
        assertEquals(SQL,  req2.identity().normalizedSql());
        assertEquals(300L, req2.contract().ttlSeconds());
    }

    // ── CacheWriteRequest ─────────────────────────────────────────────────────

    @Test
    void roundTrip_CacheWriteRequest() throws Exception {
        var req  = new CacheWriteRequest(IDENTITY, CacheContract.ttl(300L), 8192L);
        var req2 = mapper.readValue(mapper.writeValueAsString(req), CacheWriteRequest.class);
        assertEquals(SQL,   req2.identity().normalizedSql());
        assertEquals(300L,  req2.contract().ttlSeconds());
        assertEquals(8192L, req2.estimatedSizeBytes());
    }

    // ── Serialisation determinism ─────────────────────────────────────────────

    @Test
    void serialization_isDeterministic() throws Exception {
        var req   = new CacheLookupRequest(IDENTITY, CacheContract.ttl(300L));
        var json1 = mapper.writeValueAsString(req);
        var json2 = mapper.writeValueAsString(req);
        assertEquals(json1, json2);
    }

    // ── Fixture file ──────────────────────────────────────────────────────────

    @Test
    void fixture_loadsIdentity() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-cache-boundary.json")) {
            assertNotNull(in, "fixture not found");
            var node = mapper.readTree(in);
            var id   = mapper.treeToValue(node.get("identity"), CacheIdentity.class);
            assertEquals(SQL,   id.normalizedSql());
            assertEquals("alice", id.principalName());
            assertNull(id.datasetVersionToken());
            assertEquals(64, id.fingerprint().length());
        }
    }

    @Test
    void fixture_loadsContract() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-cache-boundary.json")) {
            var node = mapper.readTree(in);
            var c    = mapper.treeToValue(node.get("contract"), CacheContract.class);
            assertEquals(SemanticCacheStrategy.TTL, c.strategy());
            assertEquals(300L, c.ttlSeconds());
        }
    }

    @Test
    void fixture_loadsWriteRequest() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-cache-boundary.json")) {
            var node = mapper.readTree(in);
            var req  = mapper.treeToValue(node.get("writeRequest"), CacheWriteRequest.class);
            assertEquals(8192L, req.estimatedSizeBytes());
            assertEquals(SQL,   req.identity().normalizedSql());
        }
    }
}
