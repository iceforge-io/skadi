package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.cache.CacheContract;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;
import org.iceforge.skadi.semantic.cache.CacheLookupRequest;
import org.iceforge.skadi.semantic.cache.CacheWriteRequest;
import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Structural, validation, and no-op behavior tests for service boundary types.
 * No Spring context, no external services, no JSON serialization.
 */
class ServiceBoundaryTest {

    static final String SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";
    static final ExecutionContext CTX = ExecutionContext.of("alice");
    static final CacheIdentity IDENTITY = new CacheIdentity(SQL, "alice", null);

    // ── ExecutionContext ───────────────────────────────────────────────────────

    @Test
    void context_holdsFields() {
        var ctx = new ExecutionContext("alice", "qid-001", "pgwire");
        assertEquals("alice",   ctx.principalName());
        assertEquals("qid-001", ctx.correlationId());
        assertEquals("pgwire",  ctx.sourceSystem());
    }

    @Test
    void context_anonymous_hasEmptyPrincipal() {
        var ctx = ExecutionContext.anonymous();
        assertEquals("", ctx.principalName());
        assertNull(ctx.correlationId());
        assertNull(ctx.sourceSystem());
    }

    @Test
    void context_of_setsOnlyPrincipal() {
        var ctx = ExecutionContext.of("bob");
        assertEquals("bob", ctx.principalName());
        assertNull(ctx.correlationId());
    }

    @Test
    void context_rejectsNullPrincipal() {
        assertThrows(NullPointerException.class,
                () -> new ExecutionContext(null, null, null));
    }

    @Test
    void context_allowsEmptyPrincipal() {
        assertDoesNotThrow(() -> new ExecutionContext("", null, null));
    }

    // ── QueryExecutionRequest ─────────────────────────────────────────────────

    @Test
    void executionRequest_sqlOnly_factory() {
        var req = QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none());
        assertEquals(SQL,  req.sql());
        assertNull(req.semanticContractName());
        assertEquals(CTX, req.context());
    }

    @Test
    void executionRequest_semanticFirst() {
        var req = new QueryExecutionRequest(CTX, null, "mxl_risk", CacheContract.ttl(300L));
        assertNull(req.sql());
        assertEquals("mxl_risk", req.semanticContractName());
    }

    @Test
    void executionRequest_bothProvided_isValid() {
        assertDoesNotThrow(() ->
                new QueryExecutionRequest(CTX, SQL, "mxl_risk", CacheContract.none()));
    }

    @Test
    void executionRequest_neitherProvided_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryExecutionRequest(CTX, null, null, CacheContract.none()));
        assertThrows(IllegalArgumentException.class,
                () -> new QueryExecutionRequest(CTX, "", "  ", CacheContract.none()));
    }

    @Test
    void executionRequest_rejectsNullContext() {
        assertThrows(NullPointerException.class,
                () -> new QueryExecutionRequest(null, SQL, null, CacheContract.none()));
    }

    // ── QueryExecutionResult ──────────────────────────────────────────────────

    @Test
    void executionResult_completed_factory() {
        var r = QueryExecutionResult.completed(CTX, IDENTITY, CacheEntryState.ABSENT);
        assertEquals(ExecutionStatus.COMPLETED, r.status());
        assertEquals(IDENTITY,                  r.cacheIdentity());
        assertNull(r.outputShape());
        assertNull(r.errorMessage());
    }

    @Test
    void executionResult_cacheHit_factory() {
        var r = QueryExecutionResult.cacheHit(CTX, IDENTITY);
        assertEquals(ExecutionStatus.CACHE_HIT,  r.status());
        assertEquals(CacheEntryState.FRESH,      r.cacheState());
    }

    @Test
    void executionResult_failed_factory() {
        var r = QueryExecutionResult.failed(CTX, IDENTITY, "timeout");
        assertEquals(ExecutionStatus.FAILED, r.status());
        assertEquals("timeout",              r.errorMessage());
    }

    @Test
    void executionResult_nonFailed_rejectsErrorMessage() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryExecutionResult(CTX, ExecutionStatus.COMPLETED,
                        IDENTITY, CacheEntryState.ABSENT, null, "oops"));
    }

    @Test
    void executionResult_failed_rejectsNullErrorMessage() {
        assertThrows(NullPointerException.class,
                () -> QueryExecutionResult.failed(CTX, IDENTITY, null));
    }

    // ── QueryMetadataRequest ──────────────────────────────────────────────────

    @Test
    void metadataRequest_forSql_factory() {
        var r = QueryMetadataRequest.forSql(CTX, SQL);
        assertEquals(SQL, r.sql());
        assertNull(r.semanticContractName());
    }

    @Test
    void metadataRequest_forContract_factory() {
        var r = QueryMetadataRequest.forContract(CTX, "mxl_risk");
        assertNull(r.sql());
        assertEquals("mxl_risk", r.semanticContractName());
    }

    @Test
    void metadataRequest_neitherProvided_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryMetadataRequest(CTX, null, "  "));
    }

    // ── QueryMetadataResult ───────────────────────────────────────────────────

    @Test
    void metadataResult_unknown_hasNullFields() {
        var r = QueryMetadataResult.unknown();
        assertNull(r.outputShape());
        assertNull(r.contractName());
        assertTrue(r.references().isEmpty());
    }

    @Test
    void metadataResult_references_isUnmodifiable() {
        var r = QueryMetadataResult.unknown();
        assertThrows(UnsupportedOperationException.class,
                () -> r.references().add(null));
    }

    // ── SemanticResolutionRequest ─────────────────────────────────────────────

    @Test
    void resolutionRequest_holdsFields() {
        var r = new SemanticResolutionRequest("mxl_risk", CTX);
        assertEquals("mxl_risk", r.contractName());
        assertEquals(CTX,        r.context());
    }

    @Test
    void resolutionRequest_rejectsBlankContractName() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionRequest(" ", CTX));
    }

    // ── SemanticResolutionResult ──────────────────────────────────────────────

    static SemanticContract minimalContract() {
        var entity = new SemanticEntity("c", "", new SemanticEndpoint("cat", "sch", "tbl"), List.of());
        return new SemanticContract("c", new SemanticContractVersion("1.0.0"), "", entity,
                List.of(), List.of(), SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null);
    }

    @Test
    void resolutionResult_found_factory() {
        var c = minimalContract();
        var r = SemanticResolutionResult.found(c);
        assertTrue(r.resolved());
        assertSame(c, r.contract());
        assertNull(r.errorMessage());
    }

    @Test
    void resolutionResult_notFound_factory() {
        var r = SemanticResolutionResult.notFound("contract 'x' not accessible");
        assertFalse(r.resolved());
        assertNull(r.contract());
        assertEquals("contract 'x' not accessible", r.errorMessage());
    }

    @Test
    void resolutionResult_foundWithNullContract_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionResult(null, true, null));
    }

    @Test
    void resolutionResult_notFoundWithNonNullContract_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionResult(minimalContract(), false, null));
    }

    // ── NoOpCacheLookupService ────────────────────────────────────────────────

    @Test
    void noOpCache_lookup_returnsAbsent() {
        var svc = NoOpCacheLookupService.INSTANCE;
        var req = new CacheLookupRequest(IDENTITY, CacheContract.none());
        var result = svc.lookup(req);
        assertEquals(org.iceforge.skadi.semantic.cache.CacheEntryState.ABSENT, result.state());
        assertNull(result.artifactMeta());
    }

    @Test
    void noOpCache_write_isNoop() {
        var svc = NoOpCacheLookupService.INSTANCE;
        var req = new CacheWriteRequest(IDENTITY, CacheContract.none(), 0L);
        assertDoesNotThrow(() -> svc.write(req));
    }

    @Test
    void noOpCache_lookup_rejectsNullRequest() {
        assertThrows(NullPointerException.class,
                () -> NoOpCacheLookupService.INSTANCE.lookup(null));
    }

    // ── NoOpLineageContextProvider ────────────────────────────────────────────

    @Test
    void noOpLineage_returnsEmptyList() {
        var refs = NoOpLineageContextProvider.INSTANCE.referencesFor(CTX, "mxl_risk");
        assertNotNull(refs);
        assertTrue(refs.isEmpty());
    }

    @Test
    void noOpLineage_rejectsNullContext() {
        assertThrows(NullPointerException.class,
                () -> NoOpLineageContextProvider.INSTANCE.referencesFor(null, "c"));
    }

    @Test
    void noOpLineage_rejectsNullContractName() {
        assertThrows(NullPointerException.class,
                () -> NoOpLineageContextProvider.INSTANCE.referencesFor(CTX, null));
    }

    // ── SkadiserverQueryExecutionService ──────────────────────────────────────

    @Test
    void skadiserverSkeleton_throwsUnsupported() {
        var svc = new SkadiServerQueryExecutionService();
        var req = QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none());
        var ex  = assertThrows(UnsupportedOperationException.class, () -> svc.execute(req));
        assertTrue(ex.getMessage().contains("not yet activated"),
                "exception message should mention 'not yet activated'");
        assertTrue(ex.getMessage().contains("DQR-002"),
                "exception message should reference DQR-002");
    }

    // ── ExecutionStatus completeness ──────────────────────────────────────────

    @Test
    void executionStatus_hasExpectedValues() {
        assertEquals(4, ExecutionStatus.values().length);
    }
}
