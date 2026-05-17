package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractExplanation;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;

import java.util.List;

/**
 * Read-only API response models for the semantic metadata endpoints (Lane E1, ADR-012).
 *
 * <p>This class is a static container of record types representing the JSON shapes
 * returned by {@code /api/semantic/contracts} and related endpoints. The records map
 * from the domain model ({@link SemanticContract} and its parts) to stable,
 * API-facing representations that decouple the wire format from internal domain evolution.
 *
 * <p>Null fields are omitted from JSON output via {@link JsonInclude#NON_NULL}.
 * Optional explanation metadata fields (owner, grain, caveats, etc.) are
 * null-safe — contracts without explanation metadata produce DTOs with those
 * fields set to {@code null}, which are then excluded from the JSON response.
 *
 * <p>Factory methods ({@link #summaryFrom}, {@link #detailFrom}) are the canonical
 * way to construct these DTOs; do not construct them directly in tests.
 */
public final class SemanticContractMetadataModels {

    private SemanticContractMetadataModels() {}

    // ── Response records ───────────────────────────────────────────────────────

    /**
     * Lightweight contract summary for list responses.
     *
     * <p>Includes counts, optional ownership, and optional grain for quick
     * discoverability without exposing full measure/dimension detail.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ContractSummary(
            String name,
            String version,
            String description,
            int measureCount,
            int dimensionCount,
            String owner,
            String grain) {}

    /**
     * Full contract detail for single-contract responses.
     *
     * <p>Includes all summary fields plus cache policy, full explanation metadata,
     * and the complete measure and dimension lists.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ContractDetail(
            String name,
            String version,
            String description,
            int measureCount,
            int dimensionCount,
            String cacheStrategy,
            Long ttlSeconds,
            String owner,
            String intendedUsage,
            List<String> caveats,
            String grain,
            List<String> outputShapes,
            LineageHookDetail lineageHook,
            EntitlementBehaviorDetail entitlementBehavior,
            List<String> validGroupingPaths,
            List<MeasureDetail> measures,
            List<DimensionDetail> dimensions) {}

    /**
     * Per-measure detail including optional explanation metadata.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record MeasureDetail(
            String name,
            String label,
            String description,
            String type,
            String intendedUsage,
            List<String> caveats) {}

    /**
     * Per-dimension detail including filtering/grouping flags and optional explanation.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record DimensionDetail(
            String name,
            String column,
            String label,
            String type,
            boolean filterable,
            boolean groupable,
            String description,
            List<String> caveats) {}

    /**
     * Lineage hook placeholder — identifies the upstream lineage hook by ID.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record LineageHookDetail(
            String hookId,
            String description) {}

    /**
     * Entitlement behavior placeholder — describes how access entitlements are applied.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EntitlementBehaviorDetail(
            String mode,
            String description) {}

    // ── Factory methods ────────────────────────────────────────────────────────

    /**
     * Maps a {@link SemanticContract} to a {@link ContractSummary}.
     *
     * <p>Optional explanation fields ({@code owner}, {@code grain}) are included when
     * the contract carries explanation metadata; otherwise they are {@code null} and
     * omitted from JSON output.
     *
     * @param c the contract to map; must not be null
     */
    public static ContractSummary summaryFrom(SemanticContract c) {
        var exp = c.explanation();
        return new ContractSummary(
                c.name(),
                c.version().value(),
                c.description(),
                c.measures().size(),
                c.dimensions().size(),
                exp != null ? exp.owner() : null,
                exp != null ? exp.grain() : null);
    }

    /**
     * Maps a {@link SemanticContract} to a {@link ContractDetail}.
     *
     * <p>All explanation metadata fields are included when the contract carries
     * explanation; otherwise they are {@code null} and omitted from JSON output.
     * Measure and dimension lists are always included.
     *
     * @param c the contract to map; must not be null
     */
    public static ContractDetail detailFrom(SemanticContract c) {
        var exp = c.explanation();
        return new ContractDetail(
                c.name(),
                c.version().value(),
                c.description(),
                c.measures().size(),
                c.dimensions().size(),
                c.cachePolicy().strategy().name(),
                c.cachePolicy().ttlSeconds(),
                exp != null ? exp.owner() : null,
                exp != null ? exp.intendedUsage() : null,
                exp != null ? exp.caveats() : null,
                exp != null ? exp.grain() : null,
                exp != null ? exp.outputShapes() : null,
                lineageHookDetailFrom(exp),
                entitlementBehaviorDetailFrom(exp),
                exp != null ? exp.validGroupingPaths() : null,
                c.measures().stream().map(SemanticContractMetadataModels::measureDetailFrom).toList(),
                c.dimensions().stream().map(SemanticContractMetadataModels::dimensionDetailFrom).toList());
    }

    /**
     * Maps a {@link SemanticMeasure} to a {@link MeasureDetail}.
     *
     * @param m the measure to map; must not be null
     */
    public static MeasureDetail measureDetailFrom(SemanticMeasure m) {
        var exp = m.explanation();
        return new MeasureDetail(
                m.name(),
                m.label(),
                m.description(),
                m.type().name(),
                exp != null ? exp.intendedUsage() : null,
                exp != null ? exp.caveats() : null);
    }

    /**
     * Maps a {@link SemanticDimension} to a {@link DimensionDetail}.
     *
     * @param d the dimension to map; must not be null
     */
    public static DimensionDetail dimensionDetailFrom(SemanticDimension d) {
        var exp = d.explanation();
        return new DimensionDetail(
                d.name(),
                d.column(),
                d.label(),
                d.type().name(),
                d.filterable(),
                d.groupable(),
                exp != null ? exp.description() : null,
                exp != null ? exp.caveats() : null);
    }

    private static LineageHookDetail lineageHookDetailFrom(SemanticContractExplanation exp) {
        if (exp == null || exp.lineageHook() == null) return null;
        return new LineageHookDetail(
                exp.lineageHook().hookId(),
                exp.lineageHook().description());
    }

    private static EntitlementBehaviorDetail entitlementBehaviorDetailFrom(
            SemanticContractExplanation exp) {
        if (exp == null || exp.entitlementBehavior() == null) return null;
        return new EntitlementBehaviorDetail(
                exp.entitlementBehavior().mode(),
                exp.entitlementBehavior().description());
    }
}
