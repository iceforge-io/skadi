package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

final class MySqlToDatabricksTranslator implements SqlTranslator {
    @Override
    public TranslateResult translate(String sql, List<SqlParam> params, SqlDialectBridgeOptions opts) {
        if (sql == null) sql = "";
        if (params == null) params = List.of();

        String outSql = sql;
        List<SqlParam> outParams = params;

        if (opts.normalizeIdentifierQuotes()) {
            outSql = normalizeIdentifierQuotes(outSql);
        }

        if (opts.normalizeLimitOffset()) {
            LimitOffsetRewrite r = normalizeLimitOffset(outSql, outParams);
            outSql = r.sql();
            outParams = r.params();
        }

        ParameterMarkerRewriter.RewriteResult rr = ParameterMarkerRewriter.rewriteToJdbcQMarks(outSql, outParams, SourceDialect.MYSQL);
        return new TranslateResult(rr.sql(), rr.params());
    }

    private static String normalizeIdentifierQuotes(String sql) {
        // MySQL uses backticks already; also normalize double-quoted identifiers to backticks for Databricks.
        // Reuse the PG ident normalizer (it is dialect-agnostic, and does not touch literals/comments).
        return new PostgresToDatabricksTranslator()
                .translate(sql, List.of(), SqlDialectBridgeOptions.defaultForPgWire())
                .sql();
    }

    private record LimitOffsetRewrite(String sql, List<SqlParam> params) {
    }

    private static LimitOffsetRewrite normalizeLimitOffset(String sql, List<SqlParam> params) {
        // Canonicalize "OFFSET m LIMIT n" (MySQL) to "LIMIT n OFFSET m" when possible.
        // Additionally, when the offset/limit parts are JDBC '?' placeholders, swap their corresponding bind params.
        Objects.requireNonNull(sql, "sql");
        if (params == null) params = List.of();

        String s = sql.trim();
        String lower = s.toLowerCase(Locale.ROOT);
        int offIdx = lower.lastIndexOf(" offset ");
        int limIdx = lower.lastIndexOf(" limit ");
        if (offIdx >= 0 && limIdx > offIdx) {
            String before = s.substring(0, offIdx);
            String offPart = s.substring(offIdx + 8);
            String[] offSplit = offPart.split("(?i)\\s+limit\\s+", 2);
            if (offSplit.length == 2) {
                String offset = offSplit[0].trim();
                String limit = offSplit[1].trim();

                String rewrittenSql = before + " LIMIT " + limit + " OFFSET " + offset;

                // Param safety: if the query ends with "... OFFSET ? LIMIT ?" or similar, swap the last two params.
                // We only do this when both the offset and limit fragments are exactly "?".
                if ("?".equals(offset) && "?".equals(limit) && params.size() >= 2) {
                    List<SqlParam> out = new ArrayList<>(params);

                    int last = out.size() - 1;
                    int secondLast = out.size() - 2;

                    SqlParam pOffset = out.get(secondLast);
                    SqlParam pLimit = out.get(last);

                    // Swap values but keep original jdbcType association.
                    out.set(secondLast, new SqlParam(pOffset.index(), pLimit.jdbcType(), pLimit.value()));
                    out.set(last, new SqlParam(pLimit.index(), pOffset.jdbcType(), pOffset.value()));

                    return new LimitOffsetRewrite(rewrittenSql, out);
                }

                return new LimitOffsetRewrite(rewrittenSql, params);
            }
        }
        return new LimitOffsetRewrite(sql, params);
    }
}
