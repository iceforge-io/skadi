package org.iceforge.skadi.semantic.demo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministic rule-based implementation of {@link DemoSemanticIntentAdapter}.
 *
 * <p>Uses normalized-string matching and simple regex rules to map a small set
 * of approved demo prompts to structured {@link DemoSemanticQueryInterpretation}
 * records. No LLM, no network, no SQL generation.
 *
 * <p>Supported measures: {@code delta_exposure}, {@code dv01}, {@code vega},
 * {@code gamma}. Supported grouping dimensions: {@code desk}, {@code legal_entity},
 * {@code risk_class}, {@code risk_factor}, {@code currency}. Supported contracts:
 * {@code risk_sensitivity_exposure_grid} (GRID) and
 * {@code risk_sensitivity_historical_timeseries} (TIMESERIES).
 *
 * <p>Unknown prompts receive confidence {@link DemoSemanticInterpretationConfidence#UNSUPPORTED}.
 * Prompts with financial keywords but no resolvable measure receive
 * {@link DemoSemanticInterpretationConfidence#AMBIGUOUS}.
 *
 * <pre>{@code
 * DemoSemanticIntentAdapter adapter = RuleBasedDemoSemanticIntentAdapter.create();
 *
 * var req  = new DemoSemanticQueryRequest(
 *     "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15");
 * var interp = adapter.interpret(req);
 * // interp.contractId()  == "risk_sensitivity_exposure_grid"
 * // interp.measure()     == "delta_exposure"
 * // interp.groupBy()     == ["legal_entity", "risk_class"]
 * // interp.confidence()  == HIGH
 * }</pre>
 *
 * <p>This class has no Spring dependencies and requires no external configuration.
 */
public final class RuleBasedDemoSemanticIntentAdapter implements DemoSemanticIntentAdapter {

    // ── Contract names ─────────────────────────────────────────────────────────

    private static final String GRID_CONTRACT = "risk_sensitivity_exposure_grid";
    private static final String TS_CONTRACT   = "risk_sensitivity_historical_timeseries";

    // ── Dimension mapping (insertion-ordered: longest phrases first to avoid partial matches) ─

    private static final Map<String, String> DIMENSION_MAP;

    static {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("legal entity", "legal_entity");
        m.put("risk class",   "risk_class");
        m.put("risk factor",  "risk_factor");
        m.put("desk",         "desk");
        m.put("currency",     "currency");
        DIMENSION_MAP = Map.copyOf(m);
    }

    // ── Regex patterns ─────────────────────────────────────────────────────────

    /** Captures everything after "by" up to the first stop-word or end-of-string. */
    private static final Pattern BY_SECTION = Pattern.compile(
            "\\bby\\s+(.*?)(?=\\s+(?:for|on|over|in|with)\\b|$)");

    /** Captures a calendar date following "on" or "as of". */
    private static final Pattern DATE_FILTER = Pattern.compile(
            "(?:on|as of)\\s+(\\d{4}-\\d{2}-\\d{2})");

    // ── Factory ────────────────────────────────────────────────────────────────

    private RuleBasedDemoSemanticIntentAdapter() {}

    /**
     * Returns a new rule-based intent adapter.
     *
     * <p>The instance is stateless and thread-safe; callers may share a single instance.
     */
    public static RuleBasedDemoSemanticIntentAdapter create() {
        return new RuleBasedDemoSemanticIntentAdapter();
    }

    // ── DemoSemanticIntentAdapter ──────────────────────────────────────────────

    /**
     * Interprets {@code request} using rule-based matching.
     *
     * <p>The original {@link DemoSemanticQueryRequest#text()} is preserved verbatim in
     * {@link DemoSemanticQueryInterpretation#originalText()}. Matching is
     * case-insensitive and tolerant of minor punctuation differences.
     *
     * @param request the plain-English query request; must not be null
     * @return the interpretation; never null; isExecutable() is false for
     *         UNSUPPORTED and AMBIGUOUS confidence levels
     */
    @Override
    public DemoSemanticQueryInterpretation interpret(DemoSemanticQueryRequest request) {
        Objects.requireNonNull(request, "request must not be null");

        String original   = request.text();
        String normalized = normalize(original);

        boolean   timeseries = isTimeseries(normalized);
        String    measure    = extractMeasure(normalized);
        List<String>             groupBy = extractGroupBy(normalized);
        List<DemoSemanticFilter> filters = extractFilters(normalized);

        String                   contractId = timeseries ? TS_CONTRACT : GRID_CONTRACT;
        DemoSemanticOutputShape  shape      = timeseries
                ? DemoSemanticOutputShape.TIMESERIES
                : DemoSemanticOutputShape.GRID;

        // ── Confidence and warnings ────────────────────────────────────────────

        if (measure == null) {
            if (hasFinancialKeyword(normalized)) {
                return new DemoSemanticQueryInterpretation(
                        original, null, null, groupBy, filters, shape,
                        DemoSemanticInterpretationConfidence.AMBIGUOUS,
                        List.of("Cannot determine measure from request text. "
                                + "Specify one of: delta_exposure, dv01, vega, gamma."));
            }
            return new DemoSemanticQueryInterpretation(
                    original, null, null, List.of(), List.of(), shape,
                    DemoSemanticInterpretationConfidence.UNSUPPORTED,
                    List.of("Demo supports only market-risk sensitivity queries. "
                            + "Supported measures: delta_exposure, dv01, vega, gamma."));
        }

        List<String> warnings = new ArrayList<>();
        DemoSemanticInterpretationConfidence confidence;

        if (groupBy.isEmpty()) {
            confidence = DemoSemanticInterpretationConfidence.MEDIUM;
            warnings.add("No grouping dimensions identified. "
                    + "Specify grouping with 'by <dimension>'.");
        } else {
            confidence = DemoSemanticInterpretationConfidence.HIGH;
        }

        return new DemoSemanticQueryInterpretation(
                original, contractId, measure, groupBy, filters, shape, confidence,
                List.copyOf(warnings));
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /** Lowercases, strips punctuation, and collapses whitespace. */
    private static String normalize(String text) {
        return text.toLowerCase(Locale.ROOT)
                   .replaceAll("[.,!?;:'\"]+", " ")
                   .replaceAll("\\s+", " ")
                   .trim();
    }

    /**
     * Returns {@code true} if the normalized text contains keywords that indicate
     * a time-series (historical) query.
     */
    private static boolean isTimeseries(String n) {
        return n.contains("history")
                || n.contains("historical")
                || n.contains("trend")
                || n.contains("last 30 days")
                || n.contains("over the last")
                || n.contains("time series")
                || n.contains("timeseries");
    }

    /**
     * Extracts the measure name from the normalized text, or {@code null} when no
     * recognized measure is found.
     *
     * <p>Priority: {@code delta_exposure} > {@code dv01} > {@code vega} > {@code gamma}.
     */
    private static String extractMeasure(String n) {
        if (n.contains("delta exposure") || (n.contains("delta") && n.contains("exposure"))) {
            return "delta_exposure";
        }
        if (n.contains("dv01")) {
            return "dv01";
        }
        if (n.contains("vega")) {
            return "vega";
        }
        if (n.contains("gamma")) {
            return "gamma";
        }
        return null;
    }

    /**
     * Extracts the ordered list of group-by dimension column names from the normalized
     * text. Returns an empty list when no "by &lt;dimension&gt;" clause is found.
     *
     * <p>Handles multi-dimension clauses such as "by desk and currency".
     */
    private static List<String> extractGroupBy(String n) {
        Matcher m = BY_SECTION.matcher(n);
        if (!m.find()) return List.of();

        String   bySection = m.group(1).trim();
        String[] parts     = bySection.split("\\s+and\\s+");

        List<String> result = new ArrayList<>();
        for (String part : parts) {
            String col = DIMENSION_MAP.get(part.trim());
            if (col != null) result.add(col);
        }
        return List.copyOf(result);
    }

    /**
     * Extracts filter criteria from the normalized text.
     *
     * <p>Recognized filter patterns:
     * <ul>
     *   <li>{@code \bcib\b} → org=CIB</li>
     *   <li>{@code \busd\b} → currency=USD</li>
     *   <li>{@code \brates\b} → risk_class=rates</li>
     *   <li>{@code on YYYY-MM-DD} or {@code as of YYYY-MM-DD} → cob_date=YYYY-MM-DD</li>
     *   <li>{@code last 30 days} → date_range=last_30_days</li>
     * </ul>
     */
    private static List<DemoSemanticFilter> extractFilters(String n) {
        List<DemoSemanticFilter> filters = new ArrayList<>();

        if (n.matches(".*\\bcib\\b.*")) {
            filters.add(DemoSemanticFilter.eq("org", "CIB"));
        }
        if (n.matches(".*\\busd\\b.*")) {
            filters.add(DemoSemanticFilter.eq("currency", "USD"));
        }
        if (n.matches(".*\\brates\\b.*")) {
            filters.add(DemoSemanticFilter.eq("risk_class", "rates"));
        }

        Matcher dm = DATE_FILTER.matcher(n);
        if (dm.find()) {
            filters.add(DemoSemanticFilter.eq("cob_date", dm.group(1)));
        }

        if (n.contains("last 30 days")) {
            filters.add(DemoSemanticFilter.eq("date_range", "last_30_days"));
        }

        return List.copyOf(filters);
    }

    /**
     * Returns {@code true} if the normalized text contains at least one recognized
     * financial keyword, indicating the prompt is financial in intent but may be
     * ambiguous rather than completely unrelated.
     */
    private static boolean hasFinancialKeyword(String n) {
        return n.contains("delta")
                || n.contains("dv01")
                || n.contains("vega")
                || n.contains("gamma")
                || n.contains("exposure")
                || n.contains("sensitivity")
                || n.contains("greek");
    }
}
