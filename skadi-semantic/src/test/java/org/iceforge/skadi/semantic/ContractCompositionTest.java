package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.iceforge.skadi.semantic.cache.CacheContract;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;
import org.iceforge.skadi.semantic.cache.CacheLookupRequest;
import org.iceforge.skadi.semantic.cache.CacheStorageBackend;
import org.iceforge.skadi.semantic.cache.CacheWriteRequest;
import org.iceforge.skadi.semantic.cache.CachedArtifactMeta;
import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.contract.SemanticRuleRef;
import org.iceforge.skadi.semantic.query.SemanticOutputColumn;
import org.iceforge.skadi.semantic.query.SemanticOutputRole;
import org.iceforge.skadi.semantic.query.SemanticOutputShape;
import org.iceforge.skadi.semantic.query.SemanticQueryContract;
import org.iceforge.skadi.semantic.query.SemanticReference;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.registry.DuplicateContractException;
import org.iceforge.skadi.semantic.service.CacheLookupService;
import org.iceforge.skadi.semantic.service.ExecutionContext;
import org.iceforge.skadi.semantic.service.LineageContextProvider;
import org.iceforge.skadi.semantic.service.NoOpCacheLookupService;
import org.iceforge.skadi.semantic.service.NoOpLineageContextProvider;
import org.iceforge.skadi.semantic.service.QueryExecutionRequest;
import org.iceforge.skadi.semantic.service.QueryExecutionResult;
import org.iceforge.skadi.semantic.service.SemanticContractResolver;
import org.iceforge.skadi.semantic.service.SemanticResolutionRequest;
import org.iceforge.skadi.semantic.service.SemanticResolutionResult;
import org.iceforge.skadi.semantic.service.SkadiServerQueryExecutionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * C7 cross-lane composition tests.
 *
 * <p>Verifies that Lane C types from C2 (contracts), C3 (output-shape), C4 (cache),
 * and C5 (service interfaces) compose correctly end-to-end.  Every test is
 * deterministic, offline, and requires no Spring context, Databricks, S3, or Tableau.
 */
class ContractCompositionTest {

    // ── shared fixtures ───────────────────────────────────────────────────────

    static final String NORMALIZED_SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";
    static final String PRINCIPAL = "alice";
    static final ExecutionContext CTX = new ExecutionContext(PRINCIPAL, "qid-c7", "test");

    static SemanticContract buildContract() {
        var entity = new SemanticEntity(
                "mxl_risk", "MXL risk gold layer",
                new SemanticEndpoint("main", "risk", "gold_risk"),
                List.of(new SemanticRuleRef("BCBS239-01", "lineage requirement")));
        var pnl  = new SemanticMeasure("pnl", "Total PnL", "SUM(pnl)", SemanticFieldType.DECIMAL, "");
        var book = new SemanticDimension("book", "book", SemanticFieldType.STRING, "Book", true, true);
        return new SemanticContract(
                "mxl_risk", new SemanticContractVersion("1.0.0"),
                "MXL risk gold layer",
                entity, List.of(pnl), List.of(book),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.ttl(300L));
    }

    static SemanticQueryContract buildQueryContract() {
        var pnlCol  = new SemanticOutputColumn("pnl", "Total PnL", SemanticFieldType.DECIMAL,
                true, SemanticOutputRole.MEASURE, null, List.of());
        var bookCol = new SemanticOutputColumn("book", "Book", SemanticFieldType.STRING,
                false, SemanticOutputRole.DIMENSION, null, List.of());
        var shape = new SemanticOutputShape(List.of(bookCol, pnlCol));
        return new SemanticQueryContract(
                "mxl_risk_pnl_by_book", "PnL by book",
                "mxl_risk", new SemanticContractVersion("1.0.0"), shape, List.of());
    }

    static CacheIdentity buildIdentity() {
        return new CacheIdentity(NORMALIZED_SQL, PRINCIPAL, null);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Minimal in-memory registry backed by a plain Map for composition tests. */
    static ContractRegistry registryWith(SemanticContract... contracts) {
        var store = new LinkedHashMap<String, SemanticContract>();
        for (var c : contracts) {
            if (store.containsKey(c.name())) throw new DuplicateContractException(c.name());
            store.put(c.name(), c);
        }
        return new ContractRegistry() {
            @Override public void register(SemanticContract contract) {
                if (store.containsKey(contract.name())) throw new DuplicateContractException(contract.name());
                store.put(contract.name(), contract);
            }
            @Override public Optional<SemanticContract> findByName(String name) {
                return Optional.ofNullable(store.get(name));
            }
            @Override public List<SemanticContract> list() { return List.copyOf(store.values()); }
            @Override public List<SemanticContract> forPrincipal(String p) { return list(); }
        };
    }

    /** Inline resolver that delegates to a registry with no access-policy filtering. */
    static SemanticContractResolver resolverFor(ContractRegistry registry) {
        return request -> registry.findByName(request.contractName())
                .map(SemanticResolutionResult::found)
                .orElse(SemanticResolutionResult.notFound(
                        "contract not found: " + request.contractName()));
    }

    private ObjectMapper mapper;

    @BeforeEach
    void setUpMapper() {
        mapper = new ObjectMapper()
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // ── C2→C3→C4→C5 full composition ─────────────────────────────────────────

    @Test
    void fullComposition_contractToQueryContractToCacheIdentityToExecution() {
        var contract      = buildContract();
        var queryContract = buildQueryContract();

        // C3 links to C2 by contract name
        assertEquals(contract.name(), queryContract.sourceContract());

        // C4: derive CacheIdentity and CacheContract from the SemanticContract
        var cacheContract  = CacheContract.from(contract.cachePolicy());
        var cacheIdentity  = buildIdentity();
        assertEquals(SemanticCacheStrategy.TTL, cacheContract.strategy());
        assertEquals(300L,                      cacheContract.ttlSeconds());

        // C5: build a QueryExecutionRequest using context and cacheContract
        var req = QueryExecutionRequest.sqlOnly(CTX, NORMALIZED_SQL, cacheContract);

        // simulate a completed execution result
        var result = QueryExecutionResult.completed(CTX, cacheIdentity, CacheEntryState.ABSENT);

        assertEquals(PRINCIPAL,         result.context().principalName());
        assertEquals(cacheIdentity,     result.cacheIdentity());
        assertEquals(CacheEntryState.ABSENT, result.cacheState());
        assertNull(result.outputShape());
        assertNull(result.errorMessage());
    }

    @Test
    void composition_outputShape_fromQueryContractMatchesMeasureAndDimension() {
        var contract      = buildContract();
        var queryContract = buildQueryContract();
        var shape         = queryContract.outputShape();

        assertEquals(2, shape.columnCount());

        var pnl = shape.findColumn("pnl").orElseThrow();
        assertEquals(SemanticOutputRole.MEASURE,    pnl.role());
        assertEquals(SemanticFieldType.DECIMAL,     pnl.fieldType());

        // The query contract's measure name should appear in the source contract
        boolean measureInContract = contract.measures().stream()
                .anyMatch(m -> m.name().equals(pnl.name()));
        assertTrue(measureInContract, "output column 'pnl' should be backed by a measure in the source contract");
    }

    @Test
    void composition_cacheIdentityFingerprint_stableAcrossRebuild() {
        var id1 = buildIdentity();
        var id2 = new CacheIdentity(NORMALIZED_SQL, PRINCIPAL, null);
        assertEquals(id1.fingerprint(), id2.fingerprint());
        assertEquals(64, id1.fingerprint().length());
    }

    @Test
    void composition_cachePolicy_chainThroughCacheContract() {
        var contract       = buildContract();
        var cacheContract  = CacheContract.from(contract.cachePolicy());
        var writeRequest   = new CacheWriteRequest(buildIdentity(), cacheContract, 4096L);
        var lookupRequest  = new CacheLookupRequest(buildIdentity(), cacheContract);

        assertEquals(cacheContract.strategy(),   writeRequest.contract().strategy());
        assertEquals(cacheContract.ttlSeconds(), lookupRequest.contract().ttlSeconds());
    }

    @Test
    void composition_noOpCacheLookup_returnsAbsent_inComposedContext() {
        CacheLookupService cache = NoOpCacheLookupService.INSTANCE;
        var req    = new CacheLookupRequest(buildIdentity(), CacheContract.ttl(300L));
        var result = cache.lookup(req);

        assertEquals(CacheEntryState.ABSENT, result.state());
        assertNull(result.artifactMeta());

        // write is silent — no exception
        assertDoesNotThrow(() -> cache.write(
                new CacheWriteRequest(buildIdentity(), CacheContract.none(), 0L)));
    }

    @Test
    void composition_noOpLineageProvider_returnsEmpty_inComposedContext() {
        LineageContextProvider lineage = NoOpLineageContextProvider.INSTANCE;
        var refs = lineage.referencesFor(CTX, "mxl_risk");
        assertNotNull(refs);
        assertTrue(refs.isEmpty());
        // returned list is unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> refs.add(null));
    }

    @Test
    void composition_contractResolver_resolves_andReturnsNotFoundForMissing() {
        var contract  = buildContract();
        var registry  = registryWith(contract);
        var resolver  = resolverFor(registry);

        var found = resolver.resolve(new SemanticResolutionRequest("mxl_risk", CTX));
        assertTrue(found.resolved());
        assertEquals("mxl_risk", found.contract().name());

        var missing = resolver.resolve(new SemanticResolutionRequest("unknown_contract", CTX));
        assertFalse(missing.resolved());
        assertNull(missing.contract());
    }

    @Test
    void composition_skadiServerSkeleton_throwsUnsupported_withDqrReference() {
        var svc = new SkadiServerQueryExecutionService();
        var req = QueryExecutionRequest.sqlOnly(CTX, NORMALIZED_SQL, CacheContract.none());
        var ex  = assertThrows(UnsupportedOperationException.class, () -> svc.execute(req));
        assertTrue(ex.getMessage().contains("not yet activated"),
                "message must say 'not yet activated'");
        assertTrue(ex.getMessage().contains("DQR-002"),
                "message must reference DQR-002");
    }

    // ── Mutable-input defense: cross-lane boundary ────────────────────────────

    @Test
    void defense_mutableListToSemanticContract_doesNotAffectMeasures() {
        var mutable = new ArrayList<SemanticMeasure>();
        mutable.add(new SemanticMeasure("pnl", "PnL", "SUM(pnl)", SemanticFieldType.DECIMAL, ""));
        var entity   = new SemanticEntity("c", "", new SemanticEndpoint("a", "b", "c"), List.of());
        var contract = new SemanticContract("c", new SemanticContractVersion("1.0.0"), "", entity,
                mutable, List.of(), SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none());
        mutable.add(new SemanticMeasure("delta", "Delta", "SUM(delta)", SemanticFieldType.DECIMAL, ""));
        assertEquals(1, contract.measures().size(),
                "SemanticContract.measures must be defensively copied");
    }

    @Test
    void defense_mutableListToSemanticOutputShape_doesNotAffectColumns() {
        var col1    = new SemanticOutputColumn("c1", "c1", SemanticFieldType.STRING, false,
                SemanticOutputRole.DIMENSION, null, List.of());
        var mutable = new ArrayList<SemanticOutputColumn>();
        mutable.add(col1);
        var shape = new SemanticOutputShape(mutable, null, List.of());
        mutable.add(new SemanticOutputColumn("c2", "c2", SemanticFieldType.INTEGER, false,
                SemanticOutputRole.MEASURE, null, List.of()));
        assertEquals(1, shape.columnCount(),
                "SemanticOutputShape.columns must be defensively copied");
    }

    @Test
    void defense_mutableListToSemanticAccessPolicy_doesNotAffectRoles() {
        var mutable = new ArrayList<org.iceforge.skadi.semantic.contract.SemanticRoleRef>();
        mutable.add(new org.iceforge.skadi.semantic.contract.SemanticRoleRef("risk_analyst"));
        var policy = new SemanticAccessPolicy(mutable, List.of(), List.of());
        mutable.add(new org.iceforge.skadi.semantic.contract.SemanticRoleRef("risk_admin"));
        assertEquals(1, policy.allowedRoles().size(),
                "SemanticAccessPolicy.allowedRoles must be defensively copied");
    }

    @Test
    void defense_contractRegistryList_isSnapshot() {
        var registry = registryWith(buildContract());
        var snapshot = registry.list();
        assertEquals(1, snapshot.size());

        // registering a second contract does not affect the earlier snapshot
        var c2 = new SemanticContract("other", new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity("other", "", new SemanticEndpoint("a", "b", "c"), List.of()),
                List.of(), List.of(), SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none());
        registry.register(c2);
        assertEquals(1, snapshot.size(), "earlier snapshot must not grow after registration");
        assertEquals(2, registry.list().size());
    }

    // ── JSON fixture cross-composition ────────────────────────────────────────

    @Test
    void fixtures_contractAndQueryContract_sourceContractNamesMatch() throws Exception {
        try (InputStream contractIn = getClass().getResourceAsStream("/fixtures/sample-contract.json");
             InputStream queryIn    = getClass().getResourceAsStream("/fixtures/sample-query-contract.json")) {
            assertNotNull(contractIn, "sample-contract.json not found");
            assertNotNull(queryIn,    "sample-query-contract.json not found");

            var contract      = mapper.readValue(contractIn,  SemanticContract.class);
            var queryContract = mapper.readValue(queryIn,     SemanticQueryContract.class);

            assertEquals(contract.name(), queryContract.sourceContract(),
                    "query contract's sourceContract must match the base contract's name");
        }
    }

    @Test
    void fixtures_cacheIdentity_fingerprintMatchesRecomputed() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-cache-boundary.json")) {
            assertNotNull(in, "sample-cache-boundary.json not found");
            var node  = mapper.readTree(in);
            var id    = mapper.treeToValue(node.get("identity"), CacheIdentity.class);
            // Recompute from the deserialized fields — must match
            var rebuilt = new CacheIdentity(id.normalizedSql(), id.principalName(), id.datasetVersionToken());
            assertEquals(id.fingerprint(), rebuilt.fingerprint());
        }
    }

    @Test
    void fixtures_allThreeLoadAndAreConsistentWithEachOther() throws Exception {
        // Load contract
        SemanticContract contract;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-contract.json")) {
            contract = mapper.readValue(in, SemanticContract.class);
        }
        // Load query contract
        SemanticQueryContract queryContract;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-query-contract.json")) {
            queryContract = mapper.readValue(in, SemanticQueryContract.class);
        }
        // Load cache boundary
        CacheIdentity identity;
        CacheContract cacheContract;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-cache-boundary.json")) {
            var node  = mapper.readTree(in);
            identity      = mapper.treeToValue(node.get("identity"),  CacheIdentity.class);
            cacheContract = mapper.treeToValue(node.get("contract"),  CacheContract.class);
        }

        // Cross-consistency checks
        assertEquals(contract.name(), queryContract.sourceContract());
        assertEquals(SemanticCacheStrategy.TTL, cacheContract.strategy());
        assertEquals(64, identity.fingerprint().length());

        // Build an execution request to verify all pieces wire together
        var req = QueryExecutionRequest.sqlOnly(
                ExecutionContext.of(identity.principalName()),
                identity.normalizedSql(),
                cacheContract);
        assertEquals(identity.principalName(), req.context().principalName());
    }

    // ── Output-shape type independence ────────────────────────────────────────

    @Test
    void outputShape_hasNoJavaSqlDependency() {
        // SemanticOutputShape must not hold fields whose type is from java.sql.*
        for (var f : SemanticOutputShape.class.getDeclaredFields()) {
            assertFalse(f.getType().getName().startsWith("java.sql."),
                    "SemanticOutputShape field '" + f.getName() + "' must not be a java.sql type");
        }
    }

    @Test
    void outputColumn_hasNoJavaSqlDependency() {
        for (var f : SemanticOutputColumn.class.getDeclaredFields()) {
            assertFalse(f.getType().getName().startsWith("java.sql."),
                    "SemanticOutputColumn field '" + f.getName() + "' must not be a java.sql type");
        }
    }

    // ── Package/module boundary ───────────────────────────────────────────────

    @Test
    void packageBoundary_gatewayClassNotOnClasspath() {
        // SqlDialectBridge is in skadi-sql-gateway; must not be reachable from skadi-semantic
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridge"),
                "skadi-sql-gateway must not be a dependency of skadi-semantic");
    }

    @Test
    void packageBoundary_serverQueryServiceNotOnClasspath() {
        // QueryService is in skadi-server; must not be reachable from skadi-semantic
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.iceforge.skadi.query.QueryService"),
                "skadi-server must not be a dependency of skadi-semantic");
    }

    @Test
    void packageBoundary_coreArrowStreamerNotOnClasspath() {
        // JdbcArrowStreamer is in skadi-core; must not be reachable from skadi-semantic
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.iceforge.skadi.arrow.JdbcArrowStreamer"),
                "skadi-core must not be a dependency of skadi-semantic");
    }

    // ── CachedArtifactMeta in composition context ─────────────────────────────

    @Test
    void cachedArtifactMeta_composesWithCacheLookupResult() {
        var identity = buildIdentity();
        var meta     = new CachedArtifactMeta(
                identity, CacheStorageBackend.LOCAL, 8192L,
                Instant.now(), Instant.now().plusSeconds(300));
        var hit      = org.iceforge.skadi.semantic.cache.CacheLookupResult.hit(meta);

        assertEquals(CacheEntryState.FRESH, hit.state());
        assertEquals(8192L, hit.artifactMeta().sizeBytes());
        assertEquals(identity.fingerprint(), hit.artifactMeta().identity().fingerprint());
    }

    // ── InMemoryContractRegistry does not leak internal state ─────────────────

    @Test
    void registryList_returnsUnmodifiableView() {
        var registry = registryWith(buildContract());
        assertThrows(UnsupportedOperationException.class,
                () -> registry.list().add(buildContract()));
    }

    @Test
    void registryForPrincipal_returnsUnmodifiableView() {
        var registry = registryWith(buildContract());
        assertThrows(UnsupportedOperationException.class,
                () -> registry.forPrincipal("alice").add(buildContract()));
    }

    // ── SemanticResolutionResult invariants in composed context ───────────────

    @Test
    void resolutionResult_found_holdsContractByReference() {
        var contract = buildContract();
        var result   = SemanticResolutionResult.found(contract);
        assertSame(contract, result.contract(), "found() must hold the exact same contract instance");
    }

    @Test
    void resolutionResult_notFound_nullContractAndFalseResolved() {
        var result = SemanticResolutionResult.notFound("not found");
        assertFalse(result.resolved());
        assertNull(result.contract());
        assertEquals("not found", result.errorMessage());
    }
}
