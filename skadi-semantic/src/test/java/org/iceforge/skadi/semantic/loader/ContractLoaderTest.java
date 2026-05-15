package org.iceforge.skadi.semantic.loader;

import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JsonContractLoader}.
 *
 * <p>No Spring context, no Databricks, no S3, no SQL execution, no network I/O.
 * All file tests use JUnit 5 {@code @TempDir} or classpath resources.
 */
class ContractLoaderTest {

    private ContractLoader loader;

    @BeforeEach
    void setUp() {
        loader = JsonContractLoader.create();
    }

    // ── Classpath stream loading ───────────────────────────────────────────────

    @Test
    void load_validStream_returnsExpectedContract() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            SemanticContract c = loader.load(in);
            assertEquals("mxl_risk",   c.name());
            assertEquals("1.0.0",      c.version().value());
            assertEquals(2,            c.measures().size());
            assertEquals(3,            c.dimensions().size());
        }
    }

    @Test
    void load_stream_measuresInOrder() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            var names = loader.load(in).measures().stream().map(SemanticMeasure::name).toList();
            assertEquals(List.of("pnl", "delta_risk"), names);
        }
    }

    @Test
    void load_stream_cachePolicyIsTtl7200() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            var cache = loader.load(in).cachePolicy();
            assertEquals(SemanticCacheStrategy.TTL, cache.strategy());
            assertEquals(7200L, cache.ttlSeconds());
        }
    }

    @Test
    void load_stream_accessPolicyHasTwoRoles() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            var policy = loader.load(in).accessPolicy();
            assertEquals(2, policy.allowedRoles().size());
            assertEquals("risk_analyst", policy.allowedRoles().get(0).name());
        }
    }

    // ── Filesystem path loading ────────────────────────────────────────────────

    @Test
    void load_validPath_returnsExpectedContract(@TempDir Path tmp) throws Exception {
        Path file = copyFixtureToTempDir(tmp, "sample-contract.json", "mxl_risk.json");
        SemanticContract c = loader.load(file);
        assertEquals("mxl_risk", c.name());
        assertEquals(2, c.measures().size());
    }

    @Test
    void load_missingFile_throwsContractLoadException(@TempDir Path tmp) {
        Path missing = tmp.resolve("does-not-exist.json");
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(missing));
        assertTrue(ex.source().contains("does-not-exist.json"),
                "source should mention the missing filename");
        assertNotNull(ex.getCause());
    }

    @Test
    void load_missingFile_exceptionMessageContainsPath(@TempDir Path tmp) {
        Path missing = tmp.resolve("contract-xyz.json");
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(missing));
        assertTrue(ex.getMessage().contains("contract-xyz.json"),
                "exception message should contain the file name");
    }

    // ── Parse error cases ──────────────────────────────────────────────────────

    @Test
    void load_invalidJson_throwsContractLoadException() {
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(utf8Stream("{ not valid json {{{")));
        assertNotNull(ex.getCause(), "cause should wrap the Jackson parse error");
    }

    @Test
    void load_emptyInput_throwsContractLoadException() {
        assertThrows(ContractLoadException.class,
                () -> loader.load(utf8Stream("")));
    }

    @Test
    void load_nullName_throwsContractLoadException() {
        // SemanticContract compact constructor enforces requireNonNull on name;
        // Jackson wraps the NullPointerException in JsonMappingException.
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(utf8Stream(minimalContractJson("null"))));
        assertNotNull(ex.getCause());
    }

    @Test
    void load_blankName_throwsContractLoadException() {
        // SemanticContract compact constructor throws IllegalArgumentException for blank name.
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(utf8Stream(minimalContractJson("\"  \""))));
        assertNotNull(ex.getCause());
    }

    @Test
    void load_missingRequiredField_throwsContractLoadException() {
        // Omitting "version" causes Jackson to pass null, triggering requireNonNull.
        String noVersion = """
                {
                  "name": "test",
                  "description": "",
                  "entity": {"name":"e","description":"","endpoint":{"catalog":"c","schema":"s","table":"t"},"ruleRefs":[]},
                  "measures": [], "dimensions": [],
                  "accessPolicy": {"allowedRoles":[],"allowedPrincipals":[],"ruleRefs":[]},
                  "cachePolicy": {"strategy":"NONE","ttlSeconds":null,"ruleRefs":[]}
                }
                """;
        assertThrows(ContractLoadException.class, () -> loader.load(utf8Stream(noVersion)));
    }

    // ── loadAll ────────────────────────────────────────────────────────────────

    @Test
    void loadAll_emptyList_returnsEmptyList() {
        List<SemanticContract> result = loader.loadAll(List.of());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void loadAll_multipleFiles_preservesOrder(@TempDir Path tmp) throws Exception {
        // Write three distinct contract JSON files with different names.
        Path f1 = writeContractJson(tmp, "alpha",   "alpha.json");
        Path f2 = writeContractJson(tmp, "beta",    "beta.json");
        Path f3 = writeContractJson(tmp, "gamma",   "gamma.json");

        List<SemanticContract> contracts = loader.loadAll(List.of(f1, f2, f3));

        assertEquals(3, contracts.size());
        assertEquals("alpha", contracts.get(0).name());
        assertEquals("beta",  contracts.get(1).name());
        assertEquals("gamma", contracts.get(2).name());
    }

    @Test
    void loadAll_returnsUnmodifiableList(@TempDir Path tmp) throws Exception {
        Path f = copyFixtureToTempDir(tmp, "sample-contract.json", "c.json");
        List<SemanticContract> result = loader.loadAll(List.of(f));
        assertThrows(UnsupportedOperationException.class, () -> result.add(null));
    }

    @Test
    void loadAll_failsOnFirstBadFile(@TempDir Path tmp) throws Exception {
        Path good    = copyFixtureToTempDir(tmp, "sample-contract.json", "good.json");
        Path missing = tmp.resolve("missing.json");

        // [good, missing, good] — should throw on missing, not continue
        assertThrows(ContractLoadException.class,
                () -> loader.loadAll(List.of(good, missing, good)));
    }

    // ── Immutability ───────────────────────────────────────────────────────────

    @Test
    void load_returnedMeasures_areUnmodifiable() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            var c = loader.load(in);
            assertThrows(UnsupportedOperationException.class,
                    () -> c.measures().add(null));
        }
    }

    @Test
    void load_returnedDimensions_areUnmodifiable() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            var c = loader.load(in);
            assertThrows(UnsupportedOperationException.class,
                    () -> c.dimensions().add(null));
        }
    }

    @Test
    void load_returnedAccessPolicy_roles_areUnmodifiable() throws Exception {
        try (InputStream in = stream("/fixtures/sample-contract.json")) {
            var policy = loader.load(in).accessPolicy();
            assertThrows(UnsupportedOperationException.class,
                    () -> policy.allowedRoles().add(null));
        }
    }

    // ── Source label in error messages ─────────────────────────────────────────

    @Test
    void load_streamError_sourceIsStream() {
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(utf8Stream("!!!")));
        assertEquals("<stream>", ex.source());
        assertTrue(ex.getMessage().startsWith("<stream>:"),
                "message should start with source label");
    }

    @Test
    void load_pathError_sourceIsPathString(@TempDir Path tmp) {
        Path bad = tmp.resolve("bad.json");
        ContractLoadException ex = assertThrows(ContractLoadException.class,
                () -> loader.load(bad));
        assertEquals(bad.toString(), ex.source());
    }

    // ── Null argument guards ───────────────────────────────────────────────────

    @Test
    void load_nullPath_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> loader.load((Path) null));
    }

    @Test
    void load_nullInputStream_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> loader.load((InputStream) null));
    }

    @Test
    void loadAll_nullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> loader.loadAll(null));
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private InputStream stream(String classpathResource) {
        InputStream in = getClass().getResourceAsStream(classpathResource);
        assertNotNull(in, "classpath resource not found: " + classpathResource);
        return in;
    }

    private static InputStream utf8Stream(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }

    /** Minimal valid SemanticContract JSON with the given raw name token (e.g. {@code "\"foo\""} or {@code "null"}). */
    private static String minimalContractJson(String nameToken) {
        return """
                {
                  "name": %s,
                  "version": {"value": "1.0.0"},
                  "description": "",
                  "entity": {
                    "name": "e", "description": "",
                    "endpoint": {"catalog": "c", "schema": "s", "table": "t"},
                    "ruleRefs": []
                  },
                  "measures": [], "dimensions": [],
                  "accessPolicy": {"allowedRoles": [], "allowedPrincipals": [], "ruleRefs": []},
                  "cachePolicy": {"strategy": "NONE", "ttlSeconds": null, "ruleRefs": []}
                }
                """.formatted(nameToken);
    }

    private Path copyFixtureToTempDir(Path tmp, String fixtureResource, String destName)
            throws Exception {
        Path dest = tmp.resolve(destName);
        try (InputStream in = stream("/fixtures/" + fixtureResource)) {
            Files.copy(in, dest);
        }
        return dest;
    }

    /** Writes a minimal valid contract JSON for the given name to {@code tmp/<destName>}. */
    private static Path writeContractJson(Path tmp, String contractName, String destName)
            throws Exception {
        Path dest = tmp.resolve(destName);
        Files.writeString(dest, minimalContractJson("\"" + contractName + "\""));
        return dest;
    }
}
