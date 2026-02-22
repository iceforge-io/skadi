package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.List;
import java.util.Locale;

final class MySqlToDatabricksTranslator implements SqlTranslator {
    @Override
    public TranslateResult translate(String sql, List<SqlParam> params, SqlDialectBridgeOptions opts) {
        if (sql == null) sql = "";
        if (params == null) params = List.of();

        String out = sql;

        if (opts.normalizeIdentifierQuotes()) {
            out = normalizeIdentifierQuotes(out);
        }
        if (opts.normalizeLimitOffset()) {
            out = normalizeLimitOffset(out);
        }

        ParameterMarkerRewriter.RewriteResult rr = ParameterMarkerRewriter.rewriteToJdbcQMarks(out, params, SourceDialect.MYSQL);
        return new TranslateResult(rr.sql(), rr.params());
    }

    private static String normalizeIdentifierQuotes(String sql) {
        // Replace `identifier` with `identifier` (already), but also rewrite "" similarly.
        return new PostgresToDatabricksTranslator().translate(sql, List.of(), SqlDialectBridgeOptions.defaultForPgWire()).sql();
    }

    private static String normalizeLimitOffset(String sql) {
        // Canonicalize "OFFSET m LIMIT n" (MySQL) to "LIMIT n OFFSET m" when possible.
        // Very light heuristic; safe if we only touch the tail of the query.
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
                return before + " LIMIT " + limit + " OFFSET " + offset;
            }
        }
        return sql;
    }
}

