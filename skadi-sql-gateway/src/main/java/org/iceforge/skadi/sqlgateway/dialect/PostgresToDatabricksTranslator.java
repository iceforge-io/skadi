package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.List;
import java.util.Locale;

final class PostgresToDatabricksTranslator implements SqlTranslator {

    @Override
    public TranslateResult translate(String sql, List<SqlParam> params, SqlDialectBridgeOptions opts) {
        if (sql == null) sql = "";
        if (params == null) params = List.of();

        String out = sql;

        if (opts.rewritePgCasts()) {
            out = rewritePgCasts(out, opts);
        }
        if (opts.normalizeIdentifierQuotes()) {
            out = normalizeIdentifierQuotes(out);
        }
        if (opts.normalizeLimitOffset()) {
            out = normalizeLimitOffset(out);
        }

        // Parameter marker rewriting ($1 -> ?) must happen before JDBC.
        ParameterMarkerRewriter.RewriteResult rr = ParameterMarkerRewriter.rewriteToJdbcQMarks(out, params, SourceDialect.POSTGRES);
        return new TranslateResult(rr.sql(), rr.params());
    }

    private static String normalizeIdentifierQuotes(String sql) {
        // Replace "identifier" with `identifier` (outside string literals/comments)
        StringBuilder sb = new StringBuilder(sql.length());
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1) < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                sb.append(c);
                if (c == '\n') inLineComment = false;
                continue;
            }
            if (inBlockComment) {
                sb.append(c);
                if (c == '*' && n == '/') {
                    sb.append(n);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }

            if (!inSingle && !inDouble) {
                if (c == '-' && n == '-') {
                    sb.append(c).append(n);
                    i++;
                    inLineComment = true;
                    continue;
                }
                if (c == '/' && n == '*') {
                    sb.append(c).append(n);
                    i++;
                    inBlockComment = true;
                    continue;
                }
            }

            if (!inDouble && c == '\'') {
                if (inSingle && n == '\'') {
                    sb.append("''");
                    i++;
                    continue;
                }
                inSingle = !inSingle;
                sb.append(c);
                continue;
            }

            if (!inSingle && c == '"') {
                // flip inDouble, but emit backtick
                inDouble = !inDouble;
                sb.append('`');
                continue;
            }

            sb.append(c);
        }

        return sb.toString();
    }

    private static String normalizeLimitOffset(String sql) {
        // Keep semantics, just canonicalize some common whitespace/casing around LIMIT/OFFSET.
        // We intentionally avoid deep parsing.
        if (sql == null) return "";
        String s = sql;
        // Canonicalize LIMIT/OFFSET keywords outside literals/comments by reusing normalizer uppercasing
        // and whitespace collapsing.
        return SqlNormalizer.collapseWhitespace(s);
    }

    private static String rewritePgCasts(String sql, SqlDialectBridgeOptions opts) {
        // Minimal, token-aware rewrite of expr::type -> CAST(expr AS type)
        // Heuristic: only rewrite when we see '::' outside literals/comments.
        StringBuilder out = new StringBuilder(sql.length());

        boolean inSingle = false;
        boolean inDouble = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1) < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                out.append(c);
                if (c == '\n') inLineComment = false;
                continue;
            }
            if (inBlockComment) {
                out.append(c);
                if (c == '*' && n == '/') {
                    out.append(n);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }

            if (!inSingle && !inDouble) {
                if (c == '-' && n == '-') {
                    out.append(c).append(n);
                    i++;
                    inLineComment = true;
                    continue;
                }
                if (c == '/' && n == '*') {
                    out.append(c).append(n);
                    i++;
                    inBlockComment = true;
                    continue;
                }
            }

            if (!inDouble && c == '\'') {
                if (inSingle && n == '\'') {
                    out.append("''");
                    i++;
                    continue;
                }
                inSingle = !inSingle;
                out.append(c);
                continue;
            }
            if (!inSingle && c == '"') {
                inDouble = !inDouble;
                out.append(c);
                continue;
            }

            if (!inSingle && !inDouble && c == ':' && n == ':') {
                // Back up: collect lhs expression from the already-written output.
                int lhsEnd = out.length();
                int lhsStart = findCastLhsStart(out);
                String lhs = out.substring(lhsStart, lhsEnd).trim();

                // Consume '::'
                i++;

                // Read type token to the right
                int j = i + 1;
                while (j < sql.length() && Character.isWhitespace(sql.charAt(j))) j++;
                int typeStart = j;
                while (j < sql.length()) {
                    char tc = sql.charAt(j);
                    if (Character.isLetterOrDigit(tc) || tc == '_') {
                        j++;
                        continue;
                    }
                    break;
                }
                String type = sql.substring(typeStart, j);
                String mapped = mapType(type, opts);

                // Replace lhs in output with CAST(lhs AS type)
                out.setLength(lhsStart);
                out.append("CAST(").append(lhs).append(" AS ").append(mapped).append(')');

                // Continue from j-1 (loop will i++)
                i = j - 1;
                continue;
            }

            out.append(c);
        }

        return out.toString();
    }

    private static int findCastLhsStart(StringBuilder out) {
        // Heuristic: walk backwards to find the start of the expression being cast.
        // If the expression ends with ')', include the entire parenthesized expression.
        int depth = 0;
        for (int i = out.length() - 1; i >= 0; i--) {
            char c = out.charAt(i);

            if (c == ')') {
                depth++;
                continue;
            }
            if (c == '(') {
                if (depth == 0) {
                    return i;
                }
                depth--;
                if (depth == 0) {
                    // include this '(' as part of expression
                    return i;
                }
                continue;
            }

            if (depth == 0 && (Character.isWhitespace(c) || c == ',')) {
                return i + 1;
            }
        }
        return 0;
    }

    private static String mapType(String type, SqlDialectBridgeOptions opts) {
        if (!opts.mapCommonTypes() || type == null) return type;
        String lower = type.toLowerCase(Locale.ROOT);
        return switch (lower) {
            case "text", "varchar", "bpchar", "char" -> "string";
            case "timestamp", "timestamptz" -> "timestamp";
            case "date" -> "date";
            case "bytea" -> "binary";
            default -> type;
        };
    }
}
