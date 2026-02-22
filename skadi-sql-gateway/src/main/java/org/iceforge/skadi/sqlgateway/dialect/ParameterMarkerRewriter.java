package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ParameterMarkerRewriter {
    private ParameterMarkerRewriter() {
    }

    static RewriteResult rewriteToJdbcQMarks(String sql, List<SqlParam> params, SourceDialect sourceDialect) {
        Objects.requireNonNull(sourceDialect, "sourceDialect");
        if (sql == null) sql = "";
        if (params == null) params = List.of();

        return switch (sourceDialect) {
            case POSTGRES -> rewriteDollarN(sql, params);
            case MYSQL -> rewriteQMarks(sql, params);
        };
    }

    private static RewriteResult rewriteQMarks(String sql, List<SqlParam> params) {
        // Already JDBC-style. Just ensure indexes are sequential.
        List<SqlParam> outParams = new ArrayList<>(params.size());
        for (int i = 0; i < params.size(); i++) {
            SqlParam p = params.get(i);
            outParams.add(new SqlParam(i + 1, p.jdbcType(), p.value()));
        }
        return new RewriteResult(sql, outParams, Map.of());
    }

    private static RewriteResult rewriteDollarN(String sql, List<SqlParam> params) {
        Map<Integer, SqlParam> byIndex = new HashMap<>();
        for (SqlParam p : params) {
            byIndex.put(p.index(), p);
        }

        StringBuilder outSql = new StringBuilder(sql.length());
        List<Integer> refs = new ArrayList<>();

        boolean inSingle = false;
        boolean inDouble = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1) < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                outSql.append(c);
                if (c == '\n') inLineComment = false;
                continue;
            }
            if (inBlockComment) {
                outSql.append(c);
                if (c == '*' && n == '/') {
                    outSql.append(n);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }

            if (!inSingle && !inDouble) {
                if (c == '-' && n == '-') {
                    outSql.append(c).append(n);
                    i++;
                    inLineComment = true;
                    continue;
                }
                if (c == '/' && n == '*') {
                    outSql.append(c).append(n);
                    i++;
                    inBlockComment = true;
                    continue;
                }
            }

            if (!inDouble && c == '\'') {
                if (inSingle && n == '\'') {
                    outSql.append("''");
                    i++;
                    continue;
                }
                inSingle = !inSingle;
                outSql.append(c);
                continue;
            }
            if (!inSingle && c == '"') {
                inDouble = !inDouble;
                outSql.append(c);
                continue;
            }

            if (!inSingle && !inDouble && c == '$' && Character.isDigit(n)) {
                int j = i + 1;
                int num = 0;
                while (j < sql.length() && Character.isDigit(sql.charAt(j))) {
                    num = (num * 10) + (sql.charAt(j) - '0');
                    j++;
                }
                if (num > 0) {
                    outSql.append('?');
                    refs.add(num);
                    i = j - 1;
                    continue;
                }
            }

            outSql.append(c);
        }

        List<SqlParam> outParams = new ArrayList<>(refs.size());
        Map<String, String> diag = new HashMap<>();

        for (int i = 0; i < refs.size(); i++) {
            int ref = refs.get(i);
            SqlParam p = byIndex.get(ref);
            if (p == null) {
                diag.put("missingParam" + ref, "true");
                outParams.add(new SqlParam(i + 1, null, null));
            } else {
                outParams.add(new SqlParam(i + 1, p.jdbcType(), p.value()));
            }
        }

        return new RewriteResult(outSql.toString(), outParams, diag);
    }

    record RewriteResult(String sql, List<SqlParam> params, Map<String, String> diagnostics) {
    }
}
