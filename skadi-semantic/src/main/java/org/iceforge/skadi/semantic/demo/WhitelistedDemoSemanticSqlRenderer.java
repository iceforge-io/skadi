package org.iceforge.skadi.semantic.demo;

import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Allowlist-gated implementation of {@link DemoSemanticSqlRenderer}.
 *
 * <p>Converts a {@link DemoSemanticQueryInterpretation} into a
 * {@link DemoSemanticRenderedSql} using two fixed Skadi-owned SQL templates:
 * one for the exposure-grid shape and one for the historical time-series shape.
 *
 * <h2>Safety guarantees</h2>
 * <ul>
 *   <li><b>Allowlists</b> — contracts, measures, dimensions, and filter names are each
 *       validated against an explicit set before any SQL is assembled.</li>
 *   <li><b>Bind parameters</b> — all filter values appear as {@code :param_name}
 *       placeholders in the SQL text; the actual values are held in the
 *       {@link DemoSemanticRenderedSql#params()} map. The SQL text never contains
 *       raw user-supplied strings.</li>
 *   <li><b>Configured view names</b> — FROM clauses reference views from
 *       {@link DemoViewConfig}, which is operator-controlled, not user input.</li>
 *   <li><b>No execution</b> — this class assembles strings only; it never opens a
 *       JDBC connection, calls Databricks, or contacts S3.</li>
 * </ul>
 *
 * <h2>Limit handling</h2>
 * <p>The renderer applies a default row limit ({@value #DEFAULT_LIMIT}) capped at a
 * hard maximum ({@value #MAX_LIMIT}). A custom default limit may be supplied at
 * construction time and is silently capped at {@value #MAX_LIMIT}.
 *
 * <h2>Date range</h2>
 * <p>A {@code date_range=last_30_days} filter expands to a
 * {@code cob_date between :start_date and :end_date} clause. The dates are computed
 * from the {@link Clock} supplied at construction — inject a fixed clock in tests for
 * deterministic assertions.
 *
 * <pre>{@code
 * Clock fixedClock = Clock.fixed(
 *     LocalDate.of(2026, 5, 18).atStartOfDay(ZoneOffset.UTC).toInstant(),
 *     ZoneOffset.UTC);
 * DemoSemanticSqlRenderer renderer =
 *     WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), fixedClock);
 *
 * DemoSemanticRenderedSql result = renderer.render(interpretation);
 * // result.sql() contains only :param placeholders, never raw filter values
 * // result.params() contains {"org": "CIB", "currency": "USD", ...}
 * }</pre>
 */
public final class WhitelistedDemoSemanticSqlRenderer implements DemoSemanticSqlRenderer {

    // ── Constants ─────────────────────────────────────────────────────────────

    static final int DEFAULT_LIMIT = 500;
    static final int MAX_LIMIT     = 5000;

    private static final String GRID_CONTRACT = "risk_sensitivity_exposure_grid";
    private static final String TS_CONTRACT   = "risk_sensitivity_historical_timeseries";

    // ── Allowlists ─────────────────────────────────────────────────────────────

    private static final Set<String> ALLOWED_CONTRACTS = Set.of(
            GRID_CONTRACT,
            TS_CONTRACT);

    private static final Set<String> ALLOWED_MEASURES = Set.of(
            "delta_exposure",
            "dv01",
            "vega",
            "gamma");

    private static final Set<String> ALLOWED_DIMENSIONS = Set.of(
            "cob_date",
            "legal_entity",
            "business_unit",
            "desk",
            "risk_class",
            "risk_factor",
            "currency",
            "product");

    private static final Set<String> ALLOWED_FILTERS = Set.of(
            "org",
            "currency",
            "cob_date",
            "risk_class",
            "date_range");

    // ── Instance fields ────────────────────────────────────────────────────────

    private final DemoViewConfig viewConfig;
    private final Clock          clock;
    private final int            effectiveLimit;

    // ── Constructor ────────────────────────────────────────────────────────────

    private WhitelistedDemoSemanticSqlRenderer(DemoViewConfig viewConfig, Clock clock,
                                               int effectiveLimit) {
        this.viewConfig     = Objects.requireNonNull(viewConfig, "viewConfig must not be null");
        this.clock          = Objects.requireNonNull(clock,      "clock must not be null");
        this.effectiveLimit = effectiveLimit;
    }

    // ── Factories ──────────────────────────────────────────────────────────────

    /**
     * Returns a renderer with the default limit ({@value #DEFAULT_LIMIT}).
     *
     * @param viewConfig the demo view names; must not be null
     * @param clock      clock used for {@code date_range} computations; must not be null
     */
    public static WhitelistedDemoSemanticSqlRenderer create(DemoViewConfig viewConfig, Clock clock) {
        return new WhitelistedDemoSemanticSqlRenderer(viewConfig, clock, DEFAULT_LIMIT);
    }

    /**
     * Returns a renderer with a custom default limit, capped at {@value #MAX_LIMIT}.
     *
     * @param viewConfig   the demo view names; must not be null
     * @param clock        clock used for {@code date_range} computations; must not be null
     * @param defaultLimit desired default row limit; must be positive; silently capped at {@value #MAX_LIMIT}
     * @throws IllegalArgumentException if {@code defaultLimit} is zero or negative
     */
    public static WhitelistedDemoSemanticSqlRenderer create(DemoViewConfig viewConfig, Clock clock,
                                                            int defaultLimit) {
        if (defaultLimit <= 0) {
            throw new IllegalArgumentException(
                    "defaultLimit must be positive, was: " + defaultLimit);
        }
        return new WhitelistedDemoSemanticSqlRenderer(
                viewConfig, clock, Math.min(defaultLimit, MAX_LIMIT));
    }

    /**
     * Returns a renderer backed by {@link DemoViewConfig#defaults()} and
     * {@link Clock#systemDefaultZone()}.
     *
     * <p>Not suitable for tests that assert on date-range parameters — use
     * {@link #create(DemoViewConfig, Clock)} with a fixed clock instead.
     */
    public static WhitelistedDemoSemanticSqlRenderer createWithDefaults() {
        return create(DemoViewConfig.defaults(), Clock.systemDefaultZone());
    }

    // ── DemoSemanticSqlRenderer ────────────────────────────────────────────────

    /**
     * Renders {@code interpretation} into a whitelisted SQL object.
     *
     * @param interpretation the structured interpretation to render; must not be null
     * @return rendered SQL with named bind parameters; never null
     * @throws DemoSemanticSqlRenderException if any allowlist check fails or the
     *         interpretation is not executable
     */
    @Override
    public DemoSemanticRenderedSql render(DemoSemanticQueryInterpretation interpretation) {
        Objects.requireNonNull(interpretation, "interpretation must not be null");

        validate(interpretation);

        return switch (interpretation.outputShape()) {
            case GRID       -> renderGrid(interpretation);
            case TIMESERIES -> renderTimeseries(interpretation);
        };
    }

    // ── Validation ─────────────────────────────────────────────────────────────

    private static void validate(DemoSemanticQueryInterpretation i) {
        if (!i.isExecutable()) {
            throw new DemoSemanticSqlRenderException(
                    "interpretation is not executable (confidence=" + i.confidence() + ")");
        }
        if (!ALLOWED_CONTRACTS.contains(i.contractId())) {
            throw new DemoSemanticSqlRenderException(
                    "contract not on allowlist: '" + i.contractId() + "'");
        }
        if (!ALLOWED_MEASURES.contains(i.measure())) {
            throw new DemoSemanticSqlRenderException(
                    "measure not on allowlist: '" + i.measure() + "'");
        }
        for (String dim : i.groupBy()) {
            if (!ALLOWED_DIMENSIONS.contains(dim)) {
                throw new DemoSemanticSqlRenderException(
                        "groupBy dimension not on allowlist: '" + dim + "'");
            }
        }
        for (DemoSemanticFilter f : i.filters()) {
            if (!ALLOWED_FILTERS.contains(f.name())) {
                throw new DemoSemanticSqlRenderException(
                        "filter name not on allowlist: '" + f.name() + "'");
            }
        }
        if (GRID_CONTRACT.equals(i.contractId()) && i.outputShape() != DemoSemanticOutputShape.GRID) {
            throw new DemoSemanticSqlRenderException(
                    "contract '" + GRID_CONTRACT + "' requires outputShape GRID, got: " + i.outputShape());
        }
        if (TS_CONTRACT.equals(i.contractId()) && i.outputShape() != DemoSemanticOutputShape.TIMESERIES) {
            throw new DemoSemanticSqlRenderException(
                    "contract '" + TS_CONTRACT + "' requires outputShape TIMESERIES, got: " + i.outputShape());
        }
    }

    // ── Grid rendering ─────────────────────────────────────────────────────────

    /**
     * Renders the exposure-grid SQL template.
     *
     * <pre>{@code
     * select <groupBy cols>, sum(<measure>) as <measure>
     * from <exposureView>
     * where <filter col> = :<filter col> [and ...]
     * group by <groupBy cols>
     * order by <groupBy cols>
     * limit :limit
     * }</pre>
     */
    private DemoSemanticRenderedSql renderGrid(DemoSemanticQueryInterpretation i) {
        String viewName    = viewConfig.exposureView();
        String groupByCols = String.join(", ", i.groupBy());

        Map<String, Object> params      = new LinkedHashMap<>();
        List<String>        whereParts  = new ArrayList<>();

        for (DemoSemanticFilter f : i.filters()) {
            if ("date_range".equals(f.name())) {
                // Expand to a between clause (hoisted, but for GRID just inline)
                LocalDate end   = LocalDate.now(clock);
                LocalDate start = end.minusDays(30);
                whereParts.add("cob_date between :start_date and :end_date");
                params.put("start_date", start.toString());
                params.put("end_date",   end.toString());
            } else {
                whereParts.add(f.name() + " = :" + f.name());
                params.put(f.name(), f.value());
            }
        }

        params.put("limit", effectiveLimit);

        StringBuilder sql = new StringBuilder();
        sql.append("select ").append(groupByCols)
           .append(", sum(").append(i.measure()).append(") as ").append(i.measure())
           .append("\nfrom ").append(viewName);

        if (!whereParts.isEmpty()) {
            sql.append("\nwhere ").append(String.join(" and ", whereParts));
        }

        sql.append("\ngroup by ").append(groupByCols)
           .append("\norder by ").append(groupByCols)
           .append("\nlimit :limit");

        return new DemoSemanticRenderedSql(
                sql.toString(), params, i.contractId(), i.outputShape(), i.measure(), i.groupBy(),
                viewName);
    }

    // ── Time-series rendering ──────────────────────────────────────────────────

    /**
     * Renders the historical time-series SQL template.
     *
     * <pre>{@code
     * select cob_date, <groupBy cols>, sum(<measure>) as <measure>
     * from <historyView>
     * where cob_date between :start_date and :end_date
     *   and <filter col> = :<filter col> [...]
     * group by cob_date, <groupBy cols>
     * order by cob_date, <groupBy cols>
     * limit :limit
     * }</pre>
     *
     * <p>The {@code date_range} filter is hoisted to the top of the WHERE clause and
     * expanded to {@code cob_date between :start_date and :end_date}. All other filters
     * follow as {@code and <name> = :<name>}.
     */
    private DemoSemanticRenderedSql renderTimeseries(DemoSemanticQueryInterpretation i) {
        String viewName    = viewConfig.historyView();
        String groupByCols = String.join(", ", i.groupBy());

        Map<String, Object> params         = new LinkedHashMap<>();
        List<String>        otherWhere     = new ArrayList<>();
        boolean             hasDateRange   = false;

        for (DemoSemanticFilter f : i.filters()) {
            if ("date_range".equals(f.name())) {
                hasDateRange = true;
                LocalDate end   = LocalDate.now(clock);
                LocalDate start = end.minusDays(30);
                params.put("start_date", start.toString());
                params.put("end_date",   end.toString());
            } else {
                otherWhere.add(f.name() + " = :" + f.name());
                params.put(f.name(), f.value());
            }
        }

        params.put("limit", effectiveLimit);

        StringBuilder sql = new StringBuilder();
        sql.append("select cob_date, ").append(groupByCols)
           .append(", sum(").append(i.measure()).append(") as ").append(i.measure())
           .append("\nfrom ").append(viewName);

        if (hasDateRange) {
            sql.append("\nwhere cob_date between :start_date and :end_date");
            for (String clause : otherWhere) {
                sql.append("\n  and ").append(clause);
            }
        } else if (!otherWhere.isEmpty()) {
            sql.append("\nwhere ");
            for (int idx = 0; idx < otherWhere.size(); idx++) {
                if (idx > 0) sql.append("\n  and ");
                sql.append(otherWhere.get(idx));
            }
        }

        sql.append("\ngroup by cob_date, ").append(groupByCols)
           .append("\norder by cob_date, ").append(groupByCols)
           .append("\nlimit :limit");

        return new DemoSemanticRenderedSql(
                sql.toString(), params, i.contractId(), i.outputShape(), i.measure(), i.groupBy(),
                viewName);
    }
}
