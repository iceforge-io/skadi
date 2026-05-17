package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Business-facing explanation metadata for a {@link SemanticContract}.
 *
 * <p>Carries governance and documentation fields that allow the buddy chat to
 * interrogate the contract for human-readable context without maintaining its
 * own independent business definitions. All fields are optional; a null value
 * means the information is not specified for this contract version.
 *
 * <ul>
 *   <li>{@link #owner} — team or individual responsible for this contract</li>
 *   <li>{@link #intendedUsage} — guidance on when and how to use this contract</li>
 *   <li>{@link #caveats} — known limitations, ambiguities, or gotchas</li>
 *   <li>{@link #grain} — the row-level granularity of the underlying dataset</li>
 *   <li>{@link #outputShapes} — names of the typical result shapes this contract produces</li>
 *   <li>{@link #lineageHook} — placeholder identifying the upstream lineage hook</li>
 *   <li>{@link #entitlementBehavior} — placeholder describing access entitlement behavior</li>
 *   <li>{@link #validGroupingPaths} — dimension names or paths that form valid GROUP BY combinations</li>
 * </ul>
 *
 * <pre>{@code
 * var explanation = new SemanticContractExplanation(
 *     "Risk Analytics",
 *     "Use for daily PnL and Greeks reporting at book level.",
 *     List.of("Data is only available after 7 AM UTC"),
 *     "One row per book per close-of-business date",
 *     List.of("pnl_by_book", "greeks_by_desk"),
 *     new SemanticLineageHook("RISK-LIN-01", "BCBS 239 lineage"),
 *     new SemanticEntitlementBehavior("ROW_FILTER", "Filtered by book entitlement"),
 *     List.of("cob_date", "book", "cob_date,book"));
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticContractExplanation(
        String owner,
        String intendedUsage,
        List<String> caveats,
        String grain,
        List<String> outputShapes,
        SemanticLineageHook lineageHook,
        SemanticEntitlementBehavior entitlementBehavior,
        List<String> validGroupingPaths) {

    public SemanticContractExplanation {
        caveats           = caveats           == null ? null : List.copyOf(caveats);
        outputShapes      = outputShapes      == null ? null : List.copyOf(outputShapes);
        validGroupingPaths = validGroupingPaths == null ? null : List.copyOf(validGroupingPaths);
    }
}
