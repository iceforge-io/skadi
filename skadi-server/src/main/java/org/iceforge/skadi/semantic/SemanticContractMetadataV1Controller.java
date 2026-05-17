package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.SemanticContractMetadataModels.ContractDetail;
import org.iceforge.skadi.semantic.SemanticContractMetadataModels.ContractSummary;
import org.iceforge.skadi.semantic.SemanticContractMetadataModels.DimensionDetail;
import org.iceforge.skadi.semantic.SemanticContractMetadataModels.MeasureDetail;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read-only semantic metadata API endpoints for ADR-012 buddy-chat and dashboard consumers.
 *
 * <h2>Endpoints</h2>
 * <ul>
 *   <li>{@code GET /api/semantic/v1/contracts} — all contracts as summaries</li>
 *   <li>{@code GET /api/semantic/v1/contracts/{contractId}} — full contract detail; 404 if unknown</li>
 *   <li>{@code GET /api/semantic/v1/contracts/{contractId}/measures} — all measures; 404 if contract unknown</li>
 *   <li>{@code GET /api/semantic/v1/contracts/{contractId}/measures/{measureId}} — one measure; 404 if unknown</li>
 *   <li>{@code GET /api/semantic/v1/contracts/{contractId}/dimensions} — all dimensions; 404 if contract unknown</li>
 *   <li>{@code GET /api/semantic/v1/contracts/{contractId}/dimensions/{dimensionId}} — one dimension; 404 if unknown</li>
 * </ul>
 *
 * <p>These endpoints are <strong>read-only</strong>. They do not execute SQL, call Databricks,
 * enforce entitlements, or expose physical table details beyond what the semantic contract
 * already declares. DTOs are produced by {@link SemanticContractMetadataModels}.
 */
@RestController
@RequestMapping("/api/semantic/v1/contracts")
public class SemanticContractMetadataV1Controller {

    private final ContractRegistry registry;

    public SemanticContractMetadataV1Controller(ContractRegistry registry) {
        this.registry = registry;
    }

    /**
     * Lists all loaded contracts as summaries.
     *
     * <p>Response is a JSON array of {@link ContractSummary} objects. Optional explanation
     * fields ({@code owner}, {@code grain}) are included only when the contract carries them.
     */
    @GetMapping(produces = "application/json")
    public List<ContractSummary> list() {
        return registry.list().stream()
                .map(SemanticContractMetadataModels::summaryFrom)
                .toList();
    }

    /**
     * Returns full detail for the named contract, or 404 if not found.
     */
    @GetMapping(value = "/{contractId}", produces = "application/json")
    public ResponseEntity<?> getContract(@PathVariable String contractId) {
        return registry.findByName(contractId)
                .<ResponseEntity<?>>map(c -> ResponseEntity.ok(SemanticContractMetadataModels.detailFrom(c)))
                .orElseGet(() -> notFound("contract", contractId));
    }

    /**
     * Returns all measures for the named contract, or 404 if the contract is not found.
     */
    @GetMapping(value = "/{contractId}/measures", produces = "application/json")
    public ResponseEntity<?> getMeasures(@PathVariable String contractId) {
        Optional<SemanticContract> found = registry.findByName(contractId);
        if (found.isEmpty()) return notFound("contract", contractId);
        List<MeasureDetail> measures = found.get().measures().stream()
                .map(SemanticContractMetadataModels::measureDetailFrom)
                .toList();
        return ResponseEntity.ok(measures);
    }

    /**
     * Returns one measure by name, or 404 if the contract or measure is not found.
     */
    @GetMapping(value = "/{contractId}/measures/{measureId}", produces = "application/json")
    public ResponseEntity<?> getMeasure(@PathVariable String contractId,
                                        @PathVariable String measureId) {
        Optional<SemanticContract> found = registry.findByName(contractId);
        if (found.isEmpty()) return notFound("contract", contractId);
        return found.get().measures().stream()
                .filter(m -> m.name().equals(measureId))
                .<ResponseEntity<?>>map(m -> ResponseEntity.ok(SemanticContractMetadataModels.measureDetailFrom(m)))
                .findFirst()
                .orElseGet(() -> notFound("measure", measureId));
    }

    /**
     * Returns all dimensions for the named contract, or 404 if the contract is not found.
     */
    @GetMapping(value = "/{contractId}/dimensions", produces = "application/json")
    public ResponseEntity<?> getDimensions(@PathVariable String contractId) {
        Optional<SemanticContract> found = registry.findByName(contractId);
        if (found.isEmpty()) return notFound("contract", contractId);
        List<DimensionDetail> dimensions = found.get().dimensions().stream()
                .map(SemanticContractMetadataModels::dimensionDetailFrom)
                .toList();
        return ResponseEntity.ok(dimensions);
    }

    /**
     * Returns one dimension by name, or 404 if the contract or dimension is not found.
     */
    @GetMapping(value = "/{contractId}/dimensions/{dimensionId}", produces = "application/json")
    public ResponseEntity<?> getDimension(@PathVariable String contractId,
                                          @PathVariable String dimensionId) {
        Optional<SemanticContract> found = registry.findByName(contractId);
        if (found.isEmpty()) return notFound("contract", contractId);
        return found.get().dimensions().stream()
                .filter(d -> d.name().equals(dimensionId))
                .<ResponseEntity<?>>map(d -> ResponseEntity.ok(SemanticContractMetadataModels.dimensionDetailFrom(d)))
                .findFirst()
                .orElseGet(() -> notFound("dimension", dimensionId));
    }

    private static ResponseEntity<?> notFound(String kind, String id) {
        return ResponseEntity.status(404)
                .body(Map.of("error", kind + " not found", "name", id));
    }
}
