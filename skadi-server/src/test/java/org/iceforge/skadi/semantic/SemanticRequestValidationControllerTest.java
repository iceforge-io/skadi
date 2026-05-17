package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractExplanation;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticEntitlementBehavior;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticLineageHook;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.request.SemanticValidationRequestType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for {@link SemanticRequestValidationController}.
 *
 * <p>Uses MockMvc standalone — no Spring context, no Databricks, no SQL execution.
 */
class SemanticRequestValidationControllerTest {

    private static final String URL = "/api/semantic/v1/query/validate";

    private MockMvc mockMvc;
    private ContractRegistry registry;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        registry = Mockito.mock(ContractRegistry.class);
        mockMvc = MockMvcBuilders
                .standaloneSetup(new SemanticRequestValidationController(registry))
                .build();
        mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private String body(String contractId, SemanticValidationRequestType type, String subjectId)
            throws Exception {
        return mapper.writeValueAsString(
                new org.iceforge.skadi.semantic.request.SemanticValidationRequest(
                        contractId, type, subjectId));
    }

    private static SemanticContract minimal(String name) {
        var entity = new SemanticEntity(name, "", new SemanticEndpoint("c", "s", name), List.of());
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                entity,
                List.of(new SemanticMeasure("pnl", "PnL", "SUM(pnl)",
                        SemanticFieldType.DECIMAL, "", null)),
                List.of(
                        new SemanticDimension("book", "book", SemanticFieldType.STRING,
                                "Book", true, true, null),
                        new SemanticDimension("desk", "desk", SemanticFieldType.STRING,
                                "Desk", true, false, null)),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null);
    }

    private static SemanticContract enriched(String name) {
        var exp = new SemanticContractExplanation(
                "Risk", "Daily VaR.", List.of("After 8 AM"),
                "One row per legal entity and COB date",
                List.of("var_by_entity", "var_by_risk_class"),
                new SemanticLineageHook("H1", "lineage"),
                new SemanticEntitlementBehavior("ROW_FILTER", "row filter"),
                List.of("cob_date", "legal_entity", "cob_date,legal_entity"));
        var entity = new SemanticEntity(name, "", new SemanticEndpoint("c", "s", name), List.of());
        return new SemanticContract(
                name, new SemanticContractVersion("2.0.0"), "", entity,
                List.of(new SemanticMeasure("var_1d", "1-Day VaR", "SUM(v)",
                        SemanticFieldType.DECIMAL, "", null)),
                List.of(new SemanticDimension("legal_entity", "le_code",
                        SemanticFieldType.STRING, "Legal Entity", true, true, null)),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), exp);
    }

    // ── Unknown contract ───────────────────────────────────────────────────────

    @Test
    void unknownContract_returnsAllowedFalse_withUnknownContractCode() throws Exception {
        when(registry.findByName("ghost")).thenReturn(Optional.empty());
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("ghost", SemanticValidationRequestType.MEASURE_ACCESS, "pnl")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNKNOWN_CONTRACT"))
                .andExpect(jsonPath("$.contractId").doesNotExist());
    }

    // ── MEASURE_ACCESS ─────────────────────────────────────────────────────────

    @Test
    void measureAccess_knownMeasure_returnsAllowed() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.MEASURE_ACCESS, "pnl")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ALLOWED"))
                .andExpect(jsonPath("$.contractId").value("mxl_risk"));
    }

    @Test
    void measureAccess_unknownMeasure_returnsDenied() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.MEASURE_ACCESS, "nonexistent")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNKNOWN_MEASURE"))
                .andExpect(jsonPath("$.contractId").value("mxl_risk"))
                .andExpect(jsonPath("$.invalidFields[0]").value("nonexistent"));
    }

    // ── DIMENSION_GROUPING ─────────────────────────────────────────────────────

    @Test
    void dimensionGrouping_groupableDimension_returnsAllowed() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.DIMENSION_GROUPING, "book")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ALLOWED"));
    }

    @Test
    void dimensionGrouping_nonGroupableDimension_returnsDenied() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.DIMENSION_GROUPING, "desk")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("DIMENSION_NOT_GROUPABLE"))
                .andExpect(jsonPath("$.invalidFields[0]").value("desk"));
    }

    @Test
    void dimensionGrouping_unknownDimension_returnsDenied() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.DIMENSION_GROUPING, "ghost")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNKNOWN_DIMENSION"))
                .andExpect(jsonPath("$.invalidFields[0]").value("ghost"));
    }

    // ── DIMENSION_FILTER ───────────────────────────────────────────────────────

    @Test
    void dimensionFilter_filterableDimension_returnsAllowed() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.DIMENSION_FILTER, "desk")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ALLOWED"));
    }

    @Test
    void dimensionFilter_unknownDimension_returnsDenied() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.DIMENSION_FILTER, "ghost")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNKNOWN_DIMENSION"));
    }

    // ── GROUPING_PATH ──────────────────────────────────────────────────────────

    @Test
    void groupingPath_validPath_returnsAllowed() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("cib_var", SemanticValidationRequestType.GROUPING_PATH, "cob_date,legal_entity")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ALLOWED"));
    }

    @Test
    void groupingPath_invalidPath_returnsDenied() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("cib_var", SemanticValidationRequestType.GROUPING_PATH, "desk,legal_entity")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("INVALID_GROUPING_PATH"))
                .andExpect(jsonPath("$.invalidFields[0]").value("desk,legal_entity"));
    }

    @Test
    void groupingPath_noPathsDeclared_returnsUnsupportedRequest() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.GROUPING_PATH, "book")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNSUPPORTED_REQUEST"));
    }

    // ── OUTPUT_SHAPE ───────────────────────────────────────────────────────────

    @Test
    void outputShape_supportedShape_returnsAllowed() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("cib_var", SemanticValidationRequestType.OUTPUT_SHAPE, "var_by_entity")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ALLOWED"));
    }

    @Test
    void outputShape_unsupportedShape_returnsDenied() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("cib_var", SemanticValidationRequestType.OUTPUT_SHAPE, "var_by_desk")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNSUPPORTED_OUTPUT_SHAPE"))
                .andExpect(jsonPath("$.invalidFields[0]").value("var_by_desk"));
    }

    @Test
    void outputShape_noShapesDeclared_returnsUnsupportedRequest() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.OUTPUT_SHAPE, "pnl_view")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNSUPPORTED_REQUEST"));
    }

    // ── GRAIN ──────────────────────────────────────────────────────────────────

    @Test
    void grain_matchingGrain_returnsAllowed() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("cib_var", SemanticValidationRequestType.GRAIN,
                                "One row per legal entity and COB date")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ALLOWED"));
    }

    @Test
    void grain_mismatchedGrain_returnsDenied() throws Exception {
        when(registry.findByName("cib_var")).thenReturn(Optional.of(enriched("cib_var")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("cib_var", SemanticValidationRequestType.GRAIN, "daily aggregate")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNSUPPORTED_GRAIN"));
    }

    @Test
    void grain_noGrainDeclared_returnsUnsupportedRequest() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.GRAIN, "daily")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.reasonCode").value("UNSUPPORTED_REQUEST"));
    }

    // ── JSON response shape ────────────────────────────────────────────────────

    @Test
    void allowedOutcome_nullFieldsOmittedFromResponse() throws Exception {
        when(registry.findByName("mxl_risk")).thenReturn(Optional.of(minimal("mxl_risk")));
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("mxl_risk", SemanticValidationRequestType.MEASURE_ACCESS, "pnl")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.invalidFields").doesNotExist());
    }

    @Test
    void deniedOutcome_unknownContract_noContractIdInResponse() throws Exception {
        when(registry.findByName("x")).thenReturn(Optional.empty());
        mockMvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON)
                        .content(body("x", SemanticValidationRequestType.MEASURE_ACCESS, "m")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.contractId").doesNotExist())
                .andExpect(jsonPath("$.invalidFields").doesNotExist());
    }
}
