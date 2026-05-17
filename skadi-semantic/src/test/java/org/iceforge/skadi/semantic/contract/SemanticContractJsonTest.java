package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JSON serialization/deserialization tests for semantic contract records.
 *
 * <p>Verifies round-trip fidelity and fixture loading. No Spring context,
 * no external services, no YAML loading, no JSON Schema validation.
 *
 * <p>Jackson 2.14+ handles Java records natively via {@code RecordComponent.getName()};
 * no {@code @JsonProperty} annotations or {@code --parameters} compiler flag are needed.
 */
class SemanticContractJsonTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        // AUTO_DETECT_IS_GETTERS disabled so that SemanticAccessPolicy.isEmpty()
        // is not mistaken for a "empty" JSON property. Record component accessors
        // (name(), label(), etc.) are detected via Jackson's native record support
        // (RecordComponent.getName()), not through the bean-getter mechanism.
        mapper = new ObjectMapper()
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    static SemanticContract buildContract() {
        var entity = new SemanticEntity(
                "mxl_risk",
                "MXL risk gold layer entity",
                new SemanticEndpoint("main", "risk", "gold_risk"),
                List.of(new SemanticRuleRef("BCBS239-01", "Data lineage requirement")));

        var measures = List.of(
                new SemanticMeasure("pnl", "Total PnL", "SUM(pnl)",
                        SemanticFieldType.DECIMAL, "Sum of daily profit and loss", null),
                new SemanticMeasure("delta_risk", "Delta Risk", "SUM(delta)",
                        SemanticFieldType.DECIMAL, "Sum of delta sensitivities", null));

        var dimensions = List.of(
                new SemanticDimension("cob_date", "cob_date", SemanticFieldType.DATE,
                        "Close of Business Date", true, true, null),
                new SemanticDimension("book", "book", SemanticFieldType.STRING,
                        "Trading Book", true, true, null),
                new SemanticDimension("desk", "desk", SemanticFieldType.STRING,
                        "Trading Desk", true, false, null));

        var access = new SemanticAccessPolicy(
                List.of(new SemanticRoleRef("risk_analyst"), new SemanticRoleRef("risk_viewer")),
                List.of(new SemanticPrincipalRef(SemanticPrincipalType.USER, "alice"),
                        new SemanticPrincipalRef(SemanticPrincipalType.GROUP, "risk-team")),
                List.of(new SemanticRuleRef("GOV-042", "Risk data governance policy")));

        var cache = SemanticCachePolicy.ttl(7200L);

        return new SemanticContract(
                "mxl_risk",
                new SemanticContractVersion("1.0.0"),
                "MXL risk gold layer — daily PnL and Greeks",
                entity, measures, dimensions, access, cache, null);
    }

    // ── SemanticContract round-trip ───────────────────────────────────────────

    @Test
    void roundTrip_SemanticContract() throws Exception {
        var original = buildContract();
        var json     = mapper.writeValueAsString(original);
        var restored = mapper.readValue(json, SemanticContract.class);

        assertEquals(original.name(),        restored.name());
        assertEquals(original.version(),     restored.version());
        assertEquals(original.description(), restored.description());
        assertEquals(original.measures(),    restored.measures());
        assertEquals(original.dimensions(),  restored.dimensions());
    }

    @Test
    void roundTrip_measures_preservedInOrder() throws Exception {
        var original = buildContract();
        var restored = mapper.readValue(
                mapper.writeValueAsString(original), SemanticContract.class);

        var orig = original.measures().stream().map(SemanticMeasure::name).toList();
        var rest = restored.measures().stream().map(SemanticMeasure::name).toList();
        assertEquals(orig, rest);
    }

    @Test
    void roundTrip_dimensions_preservedInOrder() throws Exception {
        var original = buildContract();
        var restored = mapper.readValue(
                mapper.writeValueAsString(original), SemanticContract.class);

        var orig = original.dimensions().stream().map(SemanticDimension::name).toList();
        var rest = restored.dimensions().stream().map(SemanticDimension::name).toList();
        assertEquals(orig, rest);
    }

    @Test
    void serialization_isDeterministic() throws Exception {
        var contract = buildContract();
        var json1 = mapper.writeValueAsString(contract);
        var json2 = mapper.writeValueAsString(contract);
        assertEquals(json1, json2);
    }

    // ── SemanticEntity / SemanticEndpoint ─────────────────────────────────────

    @Test
    void roundTrip_SemanticEntity() throws Exception {
        var entity = new SemanticEntity(
                "mxl_risk", "desc",
                new SemanticEndpoint("main", "risk", "gold_risk"),
                List.of(new SemanticRuleRef("R1", "rule one")));
        var json     = mapper.writeValueAsString(entity);
        var restored = mapper.readValue(json, SemanticEntity.class);

        assertEquals(entity.name(),            restored.name());
        assertEquals(entity.endpoint(),        restored.endpoint());
        assertEquals(entity.ruleRefs().size(), restored.ruleRefs().size());
        assertEquals("R1",                     restored.ruleRefs().get(0).ruleId());
    }

    @Test
    void roundTrip_SemanticEndpoint() throws Exception {
        var ep  = new SemanticEndpoint("cat", "sch", "tbl");
        var ep2 = mapper.readValue(mapper.writeValueAsString(ep), SemanticEndpoint.class);
        assertEquals(ep, ep2);
        assertEquals("cat.sch.tbl", ep2.fullyQualified());
    }

    // ── SemanticMeasure / SemanticDimension ───────────────────────────────────

    @Test
    void roundTrip_SemanticMeasure() throws Exception {
        var m  = new SemanticMeasure("pnl", "Total PnL", "SUM(pnl)",
                SemanticFieldType.DECIMAL, "desc", null);
        var m2 = mapper.readValue(mapper.writeValueAsString(m), SemanticMeasure.class);
        assertEquals(m, m2);
        assertEquals(SemanticFieldType.DECIMAL, m2.type());
    }

    @Test
    void roundTrip_SemanticDimension_filterableAndGroupable() throws Exception {
        var d  = new SemanticDimension("book", "book", SemanticFieldType.STRING,
                "Trading Book", true, false, null);
        var d2 = mapper.readValue(mapper.writeValueAsString(d), SemanticDimension.class);
        assertEquals(d, d2);
        assertTrue(d2.filterable());
        assertFalse(d2.groupable());
    }

    @Test
    void roundTrip_allSemanticFieldTypes() throws Exception {
        for (var type : SemanticFieldType.values()) {
            var m  = new SemanticMeasure("m", "l", "EXPR", type, "", null);
            var m2 = mapper.readValue(mapper.writeValueAsString(m), SemanticMeasure.class);
            assertEquals(type, m2.type());
        }
    }

    // ── SemanticAccessPolicy ──────────────────────────────────────────────────

    @Test
    void roundTrip_SemanticAccessPolicy_unrestricted() throws Exception {
        var p  = SemanticAccessPolicy.unrestricted();
        var p2 = mapper.readValue(mapper.writeValueAsString(p), SemanticAccessPolicy.class);
        assertTrue(p2.allowedRoles().isEmpty());
        assertTrue(p2.allowedPrincipals().isEmpty());
        assertTrue(p2.ruleRefs().isEmpty());
        assertTrue(p2.isEmpty());
    }

    @Test
    void roundTrip_SemanticAccessPolicy_withRolesAndPrincipals() throws Exception {
        var p = new SemanticAccessPolicy(
                List.of(new SemanticRoleRef("risk_analyst")),
                List.of(new SemanticPrincipalRef(SemanticPrincipalType.GROUP, "risk-team")),
                List.of(new SemanticRuleRef("GOV-001", "policy ref")));
        var p2 = mapper.readValue(mapper.writeValueAsString(p), SemanticAccessPolicy.class);

        assertEquals(1, p2.allowedRoles().size());
        assertEquals("risk_analyst", p2.allowedRoles().get(0).name());
        assertEquals(SemanticPrincipalType.GROUP, p2.allowedPrincipals().get(0).type());
        assertEquals("risk-team", p2.allowedPrincipals().get(0).name());
        assertEquals("GOV-001", p2.ruleRefs().get(0).ruleId());
    }

    @Test
    void roundTrip_allSemanticPrincipalTypes() throws Exception {
        for (var type : SemanticPrincipalType.values()) {
            var pr  = new SemanticPrincipalRef(type, "id");
            var pr2 = mapper.readValue(
                    mapper.writeValueAsString(pr), SemanticPrincipalRef.class);
            assertEquals(type, pr2.type());
        }
    }

    // ── SemanticCachePolicy ───────────────────────────────────────────────────

    @Test
    void roundTrip_SemanticCachePolicy_none() throws Exception {
        var p  = SemanticCachePolicy.none();
        var p2 = mapper.readValue(mapper.writeValueAsString(p), SemanticCachePolicy.class);
        assertEquals(SemanticCacheStrategy.NONE, p2.strategy());
        assertNull(p2.ttlSeconds());
        assertTrue(p2.ruleRefs().isEmpty());
    }

    @Test
    void roundTrip_SemanticCachePolicy_ttl() throws Exception {
        var p  = SemanticCachePolicy.ttl(3600L);
        var p2 = mapper.readValue(mapper.writeValueAsString(p), SemanticCachePolicy.class);
        assertEquals(SemanticCacheStrategy.TTL, p2.strategy());
        assertEquals(3600L, p2.ttlSeconds());
    }

    @Test
    void roundTrip_SemanticCachePolicy_datasetVersion() throws Exception {
        var p  = new SemanticCachePolicy(SemanticCacheStrategy.DATASET_VERSION, null, List.of());
        var p2 = mapper.readValue(mapper.writeValueAsString(p), SemanticCachePolicy.class);
        assertEquals(SemanticCacheStrategy.DATASET_VERSION, p2.strategy());
        assertNull(p2.ttlSeconds());
    }

    // ── Fixture file round-trip ───────────────────────────────────────────────

    @Test
    void fixture_loadsAndDeserializesCorrectly() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-contract.json")) {
            assertNotNull(in, "fixture file not found on classpath");
            var contract = mapper.readValue(in, SemanticContract.class);

            assertEquals("mxl_risk",  contract.name());
            assertEquals("1.0.0",     contract.version().value());
            assertEquals(2,           contract.measures().size());
            assertEquals(3,           contract.dimensions().size());
        }
    }

    @Test
    void fixture_measuresHaveCorrectNames() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-contract.json")) {
            var contract = mapper.readValue(in, SemanticContract.class);
            var names    = contract.measures().stream().map(SemanticMeasure::name).toList();
            assertEquals(List.of("pnl", "delta_risk"), names);
        }
    }

    @Test
    void fixture_dimensionsHaveCorrectFilterableFlags() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-contract.json")) {
            var contract = mapper.readValue(in, SemanticContract.class);
            // "desk" dimension has groupable: false in the fixture
            var desk = contract.dimensions().stream()
                    .filter(d -> d.name().equals("desk"))
                    .findFirst()
                    .orElseThrow();
            assertTrue(desk.filterable());
            assertFalse(desk.groupable());
        }
    }

    @Test
    void fixture_accessPolicyHasTwoRolesAndTwoPrincipals() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-contract.json")) {
            var contract = mapper.readValue(in, SemanticContract.class);
            var policy   = contract.accessPolicy();
            assertEquals(2, policy.allowedRoles().size());
            assertEquals(2, policy.allowedPrincipals().size());
            assertEquals("risk_analyst", policy.allowedRoles().get(0).name());
        }
    }

    @Test
    void fixture_cachePolicyIsTtlWith7200Seconds() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-contract.json")) {
            var contract = mapper.readValue(in, SemanticContract.class);
            var cache    = contract.cachePolicy();
            assertEquals(SemanticCacheStrategy.TTL, cache.strategy());
            assertEquals(7200L, cache.ttlSeconds());
        }
    }

    @Test
    void fixture_roundTripsToEquivalentJson() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-contract.json")) {
            var contract  = mapper.readValue(in, SemanticContract.class);
            var serialized = mapper.writeValueAsString(contract);

            // Re-parse both as JsonNode trees to compare structure independent of whitespace
            JsonNode fromFixture    = mapper.readTree(
                    getClass().getResourceAsStream("/fixtures/sample-contract.json"));
            JsonNode fromSerialized = mapper.readTree(serialized);

            assertEquals(fromFixture, fromSerialized);
        }
    }

    // ── Unmodifiable lists survive round-trip ─────────────────────────────────

    @Test
    void deserializedMeasures_listIsUnmodifiable() throws Exception {
        var contract  = buildContract();
        var restored  = mapper.readValue(
                mapper.writeValueAsString(contract), SemanticContract.class);
        assertThrows(UnsupportedOperationException.class,
                () -> restored.measures().add(
                        new SemanticMeasure("x", "x", "EXPR", SemanticFieldType.INTEGER, "", null)));
    }

    @Test
    void deserializedAccessPolicy_allowedRoles_isUnmodifiable() throws Exception {
        var policy   = buildContract().accessPolicy();
        var restored = mapper.readValue(
                mapper.writeValueAsString(policy), SemanticAccessPolicy.class);
        assertThrows(UnsupportedOperationException.class,
                () -> restored.allowedRoles().add(new SemanticRoleRef("extra")));
    }
}
