package org.iceforge.skadi.semantic.request;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the semantic request validation model types.
 *
 * <p>No Spring context, no SQL, no external services.
 */
class SemanticValidationModelTest {

    // ── SemanticValidationReasonCode ───────────────────────────────────────────

    @Test
    void reasonCode_allowed_isAllowed() {
        assertTrue(SemanticValidationReasonCode.ALLOWED.isAllowed());
    }

    @Test
    void reasonCode_allDenialCodes_areNotAllowed() {
        for (var code : SemanticValidationReasonCode.values()) {
            if (code != SemanticValidationReasonCode.ALLOWED) {
                assertFalse(code.isAllowed(), code + " should not be allowed");
            }
        }
    }

    @Test
    void reasonCode_allExpectedCodesPresent() {
        var codes = List.of(SemanticValidationReasonCode.values());
        assertTrue(codes.contains(SemanticValidationReasonCode.ALLOWED));
        assertTrue(codes.contains(SemanticValidationReasonCode.UNKNOWN_CONTRACT));
        assertTrue(codes.contains(SemanticValidationReasonCode.UNKNOWN_MEASURE));
        assertTrue(codes.contains(SemanticValidationReasonCode.UNKNOWN_DIMENSION));
        assertTrue(codes.contains(SemanticValidationReasonCode.DIMENSION_NOT_GROUPABLE));
        assertTrue(codes.contains(SemanticValidationReasonCode.DIMENSION_NOT_FILTERABLE));
        assertTrue(codes.contains(SemanticValidationReasonCode.INVALID_GROUPING_PATH));
        assertTrue(codes.contains(SemanticValidationReasonCode.UNSUPPORTED_GRAIN));
        assertTrue(codes.contains(SemanticValidationReasonCode.UNSUPPORTED_OUTPUT_SHAPE));
        assertTrue(codes.contains(SemanticValidationReasonCode.DENIED_BY_ENTITLEMENT));
        assertTrue(codes.contains(SemanticValidationReasonCode.AMBIGUOUS_REQUEST));
        assertTrue(codes.contains(SemanticValidationReasonCode.UNSUPPORTED_REQUEST));
    }

    // ── SemanticValidationRequestType ──────────────────────────────────────────

    @Test
    void requestType_allExpectedTypesPresent() {
        var types = List.of(SemanticValidationRequestType.values());
        assertTrue(types.contains(SemanticValidationRequestType.MEASURE_ACCESS));
        assertTrue(types.contains(SemanticValidationRequestType.DIMENSION_GROUPING));
        assertTrue(types.contains(SemanticValidationRequestType.DIMENSION_FILTER));
        assertTrue(types.contains(SemanticValidationRequestType.GROUPING_PATH));
        assertTrue(types.contains(SemanticValidationRequestType.OUTPUT_SHAPE));
        assertTrue(types.contains(SemanticValidationRequestType.GRAIN));
    }

    // ── SemanticValidationRequest ──────────────────────────────────────────────

    @Test
    void request_holdsFields() {
        var req = new SemanticValidationRequest(
                "cib_var", SemanticValidationRequestType.DIMENSION_GROUPING, "desk");
        assertEquals("cib_var", req.contractId());
        assertEquals(SemanticValidationRequestType.DIMENSION_GROUPING, req.requestType());
        assertEquals("desk", req.subjectId());
    }

    @Test
    void request_rejectsNullContractId() {
        assertThrows(NullPointerException.class, () ->
                new SemanticValidationRequest(null, SemanticValidationRequestType.MEASURE_ACCESS, "m"));
    }

    @Test
    void request_rejectsNullRequestType() {
        assertThrows(NullPointerException.class, () ->
                new SemanticValidationRequest("c", null, "m"));
    }

    @Test
    void request_rejectsNullSubjectId() {
        assertThrows(NullPointerException.class, () ->
                new SemanticValidationRequest("c", SemanticValidationRequestType.MEASURE_ACCESS, null));
    }

    @Test
    void request_rejectsBlankContractId() {
        assertThrows(IllegalArgumentException.class, () ->
                new SemanticValidationRequest("  ", SemanticValidationRequestType.MEASURE_ACCESS, "m"));
    }

    @Test
    void request_rejectsBlankSubjectId() {
        assertThrows(IllegalArgumentException.class, () ->
                new SemanticValidationRequest("c", SemanticValidationRequestType.MEASURE_ACCESS, ""));
    }

    @Test
    void request_allRequestTypes_constructSuccessfully() {
        for (var type : SemanticValidationRequestType.values()) {
            assertDoesNotThrow(() ->
                    new SemanticValidationRequest("cib_var", type, "subject"));
        }
    }

    // ── SemanticValidationOutcome — allowed factory ────────────────────────────

    @Test
    void outcome_allowed_isAllowed() {
        var o = SemanticValidationOutcome.allowed("cib_var", "Grouping by legal_entity is permitted.");
        assertTrue(o.allowed());
        assertEquals(SemanticValidationReasonCode.ALLOWED, o.reasonCode());
        assertEquals("cib_var", o.contractId());
        assertEquals("Grouping by legal_entity is permitted.", o.message());
        assertNull(o.invalidFields());
    }

    @Test
    void outcome_allowed_rejectsNullContractId() {
        assertThrows(NullPointerException.class, () ->
                SemanticValidationOutcome.allowed(null, "msg"));
    }

    // ── SemanticValidationOutcome — denied factories ───────────────────────────

    @Test
    void outcome_denied_simple_isNotAllowed() {
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.UNKNOWN_CONTRACT,
                "Contract 'foo' is not registered.");
        assertFalse(o.allowed());
        assertEquals(SemanticValidationReasonCode.UNKNOWN_CONTRACT, o.reasonCode());
        assertEquals("Contract 'foo' is not registered.", o.message());
        assertNull(o.contractId());
        assertNull(o.invalidFields());
    }

    @Test
    void outcome_denied_withContext_holdsAllFields() {
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.DIMENSION_NOT_GROUPABLE,
                "Grouping by 'desk' is not permitted for regulatory reporting.",
                "cib_var",
                List.of("desk"));
        assertFalse(o.allowed());
        assertEquals(SemanticValidationReasonCode.DIMENSION_NOT_GROUPABLE, o.reasonCode());
        assertEquals("cib_var", o.contractId());
        assertEquals(1, o.invalidFields().size());
        assertEquals("desk", o.invalidFields().get(0));
    }

    @Test
    void outcome_denied_invalidFields_defensivelyCopied() {
        var mutable = new ArrayList<>(List.of("desk"));
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.INVALID_GROUPING_PATH, "msg", "c", mutable);
        mutable.add("book");
        assertEquals(1, o.invalidFields().size());
    }

    @Test
    void outcome_denied_invalidFields_unmodifiable() {
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.DIMENSION_NOT_FILTERABLE, "msg", "c", List.of("d"));
        assertThrows(UnsupportedOperationException.class, () -> o.invalidFields().add("extra"));
    }

    @Test
    void outcome_denied_rejectsNullReasonCode() {
        assertThrows(NullPointerException.class, () ->
                SemanticValidationOutcome.denied(null, "msg"));
    }

    @Test
    void outcome_denied_rejectsNullMessage() {
        assertThrows(NullPointerException.class, () ->
                SemanticValidationOutcome.denied(SemanticValidationReasonCode.UNKNOWN_MEASURE, null));
    }

    // ── SemanticValidationOutcome — invariants ─────────────────────────────────

    @Test
    void outcome_allowedWithDenialCode_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new SemanticValidationOutcome(
                        true, SemanticValidationReasonCode.UNKNOWN_CONTRACT, "msg", null, null));
    }

    @Test
    void outcome_deniedWithAllowedCode_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new SemanticValidationOutcome(
                        false, SemanticValidationReasonCode.ALLOWED, "msg", null, null));
    }

    // ── All denial codes — representable ──────────────────────────────────────

    @Test
    void outcome_allDenialCodes_canBeRepresented() {
        for (var code : SemanticValidationReasonCode.values()) {
            if (code == SemanticValidationReasonCode.ALLOWED) continue;
            var outcome = SemanticValidationOutcome.denied(code, "test message for " + code);
            assertFalse(outcome.allowed());
            assertEquals(code, outcome.reasonCode());
        }
    }

    @Test
    void outcome_entitlementDenial_representable() {
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.DENIED_BY_ENTITLEMENT,
                "You do not have access to cross-entity aggregation.",
                "cib_var",
                null);
        assertFalse(o.allowed());
        assertEquals(SemanticValidationReasonCode.DENIED_BY_ENTITLEMENT, o.reasonCode());
        assertEquals("cib_var", o.contractId());
        assertNull(o.invalidFields());
    }

    @Test
    void outcome_invalidGroupingPath_representable() {
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.INVALID_GROUPING_PATH,
                "The grouping 'desk,legal_entity' is not a valid combination for cib_var.",
                "cib_var",
                List.of("desk", "legal_entity"));
        assertFalse(o.allowed());
        assertEquals(2, o.invalidFields().size());
    }

    @Test
    void outcome_ambiguousRequest_representable() {
        var o = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.AMBIGUOUS_REQUEST,
                "The term 'risk' matches multiple dimensions. Please be more specific.");
        assertFalse(o.allowed());
        assertNull(o.contractId());
    }
}
