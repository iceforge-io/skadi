package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractExplanation;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticDimensionExplanation;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticEntitlementBehavior;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticLineageHook;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.contract.SemanticMeasureExplanation;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for {@link SemanticContractMetadataV1Controller}.
 *
 * <p>Uses {@code MockMvcBuilders.standaloneSetup} — no Spring context, no Databricks,
 * no file I/O, no SQL execution. The registry is a Mockito mock.
 */
class SemanticContractMetadataV1ControllerTest {

    private static final String BASE = "/api/semantic/v1/contracts";

    private MockMvc mockMvc;
    private ContractRegistry registry;

    @BeforeEach
    void setUp() {
        registry = Mockito.mock(ContractRegistry.class);
        mockMvc = MockMvcBuilders
                .standaloneSetup(new SemanticContractMetadataV1Controller(registry))
                .build();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static SemanticContract minimal(String name) {
        var entity = new SemanticEntity(name, "", new SemanticEndpoint("c", "s", name), List.of());
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), name + " description",
                entity,
                List.of(
                        new SemanticMeasure("pnl", "PnL", "SUM(pnl)",
                                SemanticFieldType.DECIMAL, "Total PnL", null),
                        new SemanticMeasure("delta", "Delta", "SUM(delta)",
                                SemanticFieldType.DECIMAL, "Total delta", null)),
                List.of(
                        new SemanticDimension("book", "book", SemanticFieldType.STRING,
                                "Trading Book", true, true, null),
                        new SemanticDimension("desk", "desk_id", SemanticFieldType.STRING,
                                "Trading Desk", true, false, null)),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.ttl(7200L), null);
    }

    private static SemanticContract enriched(String name) {
        var exp = new SemanticContractExplanation(
                "Risk Analytics",
                "Daily VaR reporting.",
                List.of("Data after 8 AM UTC"),
                "One row per legal entity and COB date",
                List.of("var_by_entity"),
                new SemanticLineageHook("RISK-LIN-01", "BCBS 239 lineage"),
                new SemanticEntitlementBehavior("ROW_FILTER", "Row filter by entity"),
                List.of("cob_date", "legal_entity"));
        var mExp = new SemanticMeasureExplanation("Capital reporting.", List.of("Normal dist only"));
        var dExp = new SemanticDimensionExplanation("Legal entity code.", List.of("Changes during reorg"));
        var entity = new SemanticEntity(name, "", new SemanticEndpoint("m", "r", name), List.of());
        return new SemanticContract(
                name, new SemanticContractVersion("2.0.0"), "CIB VaR layer",
                entity,
                List.of(new SemanticMeasure("var_1d", "1-Day VaR", "SUM(var_1d)",
                        SemanticFieldType.DECIMAL, "1-day VaR", mExp)),
                List.of(new SemanticDimension("legal_entity", "legal_entity_code",
                        SemanticFieldType.STRING, "Legal Entity", true, true, dExp)),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.ttl(14400L), exp);
    }

    // ── GET /api/semantic/v1/contracts ─────────────────────────────────────────

    @Test
    void list_emptyRegistry_returnsEmptyArray() throws Exception {
        when(registry.list()).thenReturn(List.of());
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$").isEmpty());
    }

    @Test
    void list_oneContract_returnsSummary() throws Exception {
        when(registry.list()).thenReturn(List.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("mxl_risk"))
                .andExpect(jsonPath("$[0].version").value("1.0.0"))
                .andExpect(jsonPath("$[0].measureCount").value(2))
                .andExpect(jsonPath("$[0].dimensionCount").value(2));
    }

    @Test
    void list_multipleContracts_returnsAll() throws Exception {
        when(registry.list()).thenReturn(List.of(minimal("alpha"), minimal("beta")));
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].name").value("alpha"))
                .andExpect(jsonPath("$[1].name").value("beta"));
    }

    @Test
    void list_enrichedContract_includesOwnerAndGrain() throws Exception {
        when(registry.list()).thenReturn(List.of(enriched("cib_var")));
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].owner").value("Risk Analytics"))
                .andExpect(jsonPath("$[0].grain").value("One row per legal entity and COB date"));
    }

    @Test
    void list_minimalContract_noOwnerOrGrainInResponse() throws Exception {
        when(registry.list()).thenReturn(List.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].owner").doesNotExist())
                .andExpect(jsonPath("$[0].grain").doesNotExist());
    }

    // ── GET /api/semantic/v1/contracts/{contractId} ────────────────────────────

    @Test
    void getContract_known_returnsDetail() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("mxl_risk"))
                .andExpect(jsonPath("$.cacheStrategy").value("TTL"))
                .andExpect(jsonPath("$.ttlSeconds").value(7200))
                .andExpect(jsonPath("$.measures").isArray())
                .andExpect(jsonPath("$.dimensions").isArray());
    }

    @Test
    void getContract_unknown_returns404() throws Exception {
        when(registry.findByName("unknown")).thenReturn(Optional.empty());
        mockMvc.perform(get(BASE + "/unknown"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("contract not found"))
                .andExpect(jsonPath("$.name").value("unknown"));
    }

    @Test
    void getContract_enriched_includesExplanationFields() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(get(BASE + "/cib_var"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.owner").value("Risk Analytics"))
                .andExpect(jsonPath("$.intendedUsage").value("Daily VaR reporting."))
                .andExpect(jsonPath("$.grain").exists())
                .andExpect(jsonPath("$.lineageHook.hookId").value("RISK-LIN-01"))
                .andExpect(jsonPath("$.entitlementBehavior.mode").value("ROW_FILTER"))
                .andExpect(jsonPath("$.validGroupingPaths[0]").value("cob_date"));
    }

    @Test
    void getContract_minimal_noExplanationFieldsInResponse() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.owner").doesNotExist())
                .andExpect(jsonPath("$.lineageHook").doesNotExist())
                .andExpect(jsonPath("$.entitlementBehavior").doesNotExist());
    }

    // ── GET /api/semantic/v1/contracts/{contractId}/measures ──────────────────

    @Test
    void getMeasures_knownContract_returnsAllMeasures() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/measures"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].name").value("pnl"))
                .andExpect(jsonPath("$[1].name").value("delta"));
    }

    @Test
    void getMeasures_unknownContract_returns404() throws Exception {
        when(registry.findByName("unknown")).thenReturn(Optional.empty());
        mockMvc.perform(get(BASE + "/unknown/measures"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("contract not found"));
    }

    @Test
    void getMeasures_enriched_includesExplanationFields() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(get(BASE + "/cib_var/measures"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("var_1d"))
                .andExpect(jsonPath("$[0].intendedUsage").value("Capital reporting."))
                .andExpect(jsonPath("$[0].caveats[0]").value("Normal dist only"));
    }

    // ── GET /api/semantic/v1/contracts/{contractId}/measures/{measureId} ──────

    @Test
    void getMeasure_known_returnsMeasureDetail() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/measures/pnl"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("pnl"))
                .andExpect(jsonPath("$.label").value("PnL"))
                .andExpect(jsonPath("$.type").value("DECIMAL"));
    }

    @Test
    void getMeasure_unknownContract_returns404() throws Exception {
        when(registry.findByName("unknown")).thenReturn(Optional.empty());
        mockMvc.perform(get(BASE + "/unknown/measures/pnl"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("contract not found"));
    }

    @Test
    void getMeasure_unknownMeasure_returns404() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/measures/nonexistent"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("measure not found"))
                .andExpect(jsonPath("$.name").value("nonexistent"));
    }

    @Test
    void getMeasure_withoutExplanation_noExplanationFieldsInResponse() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/measures/pnl"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.intendedUsage").doesNotExist())
                .andExpect(jsonPath("$.caveats").doesNotExist());
    }

    // ── GET /api/semantic/v1/contracts/{contractId}/dimensions ────────────────

    @Test
    void getDimensions_knownContract_returnsAllDimensions() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/dimensions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].name").value("book"))
                .andExpect(jsonPath("$[0].filterable").value(true))
                .andExpect(jsonPath("$[0].groupable").value(true))
                .andExpect(jsonPath("$[1].name").value("desk"))
                .andExpect(jsonPath("$[1].groupable").value(false));
    }

    @Test
    void getDimensions_unknownContract_returns404() throws Exception {
        when(registry.findByName("unknown")).thenReturn(Optional.empty());
        mockMvc.perform(get(BASE + "/unknown/dimensions"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("contract not found"));
    }

    @Test
    void getDimensions_enriched_includesDescriptionAndCaveats() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(get(BASE + "/cib_var/dimensions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("legal_entity"))
                .andExpect(jsonPath("$[0].description").value("Legal entity code."))
                .andExpect(jsonPath("$[0].caveats[0]").value("Changes during reorg"));
    }

    // ── GET /api/semantic/v1/contracts/{contractId}/dimensions/{dimensionId} ──

    @Test
    void getDimension_known_returnsDimensionDetail() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/dimensions/book"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("book"))
                .andExpect(jsonPath("$.column").value("book"))
                .andExpect(jsonPath("$.label").value("Trading Book"))
                .andExpect(jsonPath("$.filterable").value(true))
                .andExpect(jsonPath("$.groupable").value(true));
    }

    @Test
    void getDimension_unknownContract_returns404() throws Exception {
        when(registry.findByName("unknown")).thenReturn(Optional.empty());
        mockMvc.perform(get(BASE + "/unknown/dimensions/book"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("contract not found"));
    }

    @Test
    void getDimension_unknownDimension_returns404() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/dimensions/nonexistent"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("dimension not found"))
                .andExpect(jsonPath("$.name").value("nonexistent"));
    }

    @Test
    void getDimension_withoutExplanation_noDescriptionOrCaveatsInResponse() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/dimensions/book"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.description").doesNotExist())
                .andExpect(jsonPath("$.caveats").doesNotExist());
    }

    @Test
    void getDimension_deskGroupableFalse_returnsCorrectFlags() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(get(BASE + "/mxl_risk/dimensions/desk"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupable").value(false))
                .andExpect(jsonPath("$.filterable").value(true));
    }
}
