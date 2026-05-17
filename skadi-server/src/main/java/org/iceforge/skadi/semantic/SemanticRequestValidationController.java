package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractExplanation;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.request.SemanticValidationOutcome;
import org.iceforge.skadi.semantic.request.SemanticValidationReasonCode;
import org.iceforge.skadi.semantic.request.SemanticValidationRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Validates semantic requests against active contract metadata (ADR-012).
 *
 * <h2>Endpoint</h2>
 * <pre>POST /api/semantic/v1/query/validate</pre>
 *
 * <p>The caller provides a {@link SemanticValidationRequest} identifying the contract,
 * the type of operation, and the subject (measure name, dimension name, grouping path,
 * output shape, or grain). The response is a {@link SemanticValidationOutcome} with:
 * <ul>
 *   <li>{@code allowed} — {@code true} if the operation is permitted</li>
 *   <li>{@code reasonCode} — machine-readable outcome code</li>
 *   <li>{@code message} — user-safe explanation</li>
 *   <li>{@code contractId} — which contract was checked (when available)</li>
 *   <li>{@code invalidFields} — which fields caused denial (when applicable)</li>
 * </ul>
 *
 * <p>This endpoint does not execute queries, generate SQL, call Databricks, or enforce
 * entitlements beyond what the contract's explanation metadata declares.
 */
@RestController
@RequestMapping("/api/semantic/v1/query")
public class SemanticRequestValidationController {

    private final ContractRegistry registry;

    public SemanticRequestValidationController(ContractRegistry registry) {
        this.registry = registry;
    }

    /**
     * Validates a semantic request against the active contract registry.
     *
     * <p>Always returns HTTP 200 — the {@link SemanticValidationOutcome#allowed()} field
     * carries the validation decision. This keeps the caller's control flow simple;
     * HTTP error codes are reserved for malformed requests.
     */
    @PostMapping(value = "/validate",
            consumes = "application/json",
            produces = "application/json")
    public SemanticValidationOutcome validate(@RequestBody SemanticValidationRequest request) {
        var found = registry.findByName(request.contractId());
        if (found.isEmpty()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNKNOWN_CONTRACT,
                    "Contract '" + request.contractId() + "' is not registered.");
        }
        return evaluate(found.get(), request);
    }

    // ── Validation logic ───────────────────────────────────────────────────────

    private SemanticValidationOutcome evaluate(SemanticContract contract,
                                               SemanticValidationRequest request) {
        return switch (request.requestType()) {
            case MEASURE_ACCESS     -> validateMeasureAccess(contract, request.subjectId());
            case DIMENSION_GROUPING -> validateDimensionGrouping(contract, request.subjectId());
            case DIMENSION_FILTER   -> validateDimensionFilter(contract, request.subjectId());
            case GROUPING_PATH      -> validateGroupingPath(contract, request.subjectId());
            case OUTPUT_SHAPE       -> validateOutputShape(contract, request.subjectId());
            case GRAIN              -> validateGrain(contract, request.subjectId());
        };
    }

    private SemanticValidationOutcome validateMeasureAccess(SemanticContract contract,
                                                            String measureId) {
        boolean exists = contract.measures().stream()
                .anyMatch(m -> m.name().equals(measureId));
        if (!exists) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNKNOWN_MEASURE,
                    "Measure '" + measureId + "' is not defined in contract '" + contract.name() + "'.",
                    contract.name(), List.of(measureId));
        }
        return SemanticValidationOutcome.allowed(
                contract.name(),
                "Measure '" + measureId + "' is defined and accessible in contract '" + contract.name() + "'.");
    }

    private SemanticValidationOutcome validateDimensionGrouping(SemanticContract contract,
                                                                String dimensionId) {
        var dim = contract.dimensions().stream()
                .filter(d -> d.name().equals(dimensionId))
                .findFirst();
        if (dim.isEmpty()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNKNOWN_DIMENSION,
                    "Dimension '" + dimensionId + "' is not defined in contract '" + contract.name() + "'.",
                    contract.name(), List.of(dimensionId));
        }
        if (!dim.get().groupable()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.DIMENSION_NOT_GROUPABLE,
                    "Dimension '" + dimensionId + "' is not available for grouping in contract '" + contract.name() + "'.",
                    contract.name(), List.of(dimensionId));
        }
        return SemanticValidationOutcome.allowed(
                contract.name(),
                "Dimension '" + dimensionId + "' can be used for grouping in contract '" + contract.name() + "'.");
    }

    private SemanticValidationOutcome validateDimensionFilter(SemanticContract contract,
                                                              String dimensionId) {
        var dim = contract.dimensions().stream()
                .filter(d -> d.name().equals(dimensionId))
                .findFirst();
        if (dim.isEmpty()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNKNOWN_DIMENSION,
                    "Dimension '" + dimensionId + "' is not defined in contract '" + contract.name() + "'.",
                    contract.name(), List.of(dimensionId));
        }
        if (!dim.get().filterable()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.DIMENSION_NOT_FILTERABLE,
                    "Dimension '" + dimensionId + "' is not available as a filter in contract '" + contract.name() + "'.",
                    contract.name(), List.of(dimensionId));
        }
        return SemanticValidationOutcome.allowed(
                contract.name(),
                "Dimension '" + dimensionId + "' can be used as a filter in contract '" + contract.name() + "'.");
    }

    private SemanticValidationOutcome validateGroupingPath(SemanticContract contract,
                                                           String path) {
        SemanticContractExplanation exp = contract.explanation();
        if (exp == null || exp.validGroupingPaths() == null || exp.validGroupingPaths().isEmpty()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNSUPPORTED_REQUEST,
                    "Contract '" + contract.name() + "' does not declare valid grouping paths.",
                    contract.name(), null);
        }
        if (!exp.validGroupingPaths().contains(path)) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.INVALID_GROUPING_PATH,
                    "The grouping path '" + path + "' is not a declared valid grouping combination for contract '" + contract.name() + "'.",
                    contract.name(), List.of(path));
        }
        return SemanticValidationOutcome.allowed(
                contract.name(),
                "The grouping path '" + path + "' is a valid grouping combination for contract '" + contract.name() + "'.");
    }

    private SemanticValidationOutcome validateOutputShape(SemanticContract contract,
                                                          String shapeName) {
        SemanticContractExplanation exp = contract.explanation();
        if (exp == null || exp.outputShapes() == null || exp.outputShapes().isEmpty()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNSUPPORTED_REQUEST,
                    "Contract '" + contract.name() + "' does not declare output shapes.",
                    contract.name(), null);
        }
        if (!exp.outputShapes().contains(shapeName)) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNSUPPORTED_OUTPUT_SHAPE,
                    "Output shape '" + shapeName + "' is not declared for contract '" + contract.name() + "'.",
                    contract.name(), List.of(shapeName));
        }
        return SemanticValidationOutcome.allowed(
                contract.name(),
                "Output shape '" + shapeName + "' is supported by contract '" + contract.name() + "'.");
    }

    private SemanticValidationOutcome validateGrain(SemanticContract contract, String grain) {
        SemanticContractExplanation exp = contract.explanation();
        if (exp == null || exp.grain() == null || exp.grain().isBlank()) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNSUPPORTED_REQUEST,
                    "Contract '" + contract.name() + "' does not declare a grain.",
                    contract.name(), null);
        }
        if (!exp.grain().equalsIgnoreCase(grain)) {
            return SemanticValidationOutcome.denied(
                    SemanticValidationReasonCode.UNSUPPORTED_GRAIN,
                    "The stated grain does not match the declared grain for contract '" + contract.name() + "'.",
                    contract.name(), null);
        }
        return SemanticValidationOutcome.allowed(
                contract.name(),
                "The stated grain matches the declared grain for contract '" + contract.name() + "'.");
    }
}
