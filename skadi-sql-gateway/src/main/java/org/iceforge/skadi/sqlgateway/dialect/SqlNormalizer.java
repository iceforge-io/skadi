package org.iceforge.skadi.sqlgateway.dialect;

import java.util.Locale;

final class SqlNormalizer {
    private SqlNormalizer() {
    }

    /**
     * Normalizes SQL into a stable form suitable for cache keys.
     *
     * <p>Rules (MVP):
     * <ul>
     *   <li>Strip line and block comments</li>
     *   <li>Collapse all whitespace runs to a single space</li>
     *   <li>Trim</li>
     *   <li>Upper-case outside string literals (so casing differences don't split cache)</li>
     * </ul>
     */
    static String normalizeForKey(String sql) {
        if (sql == null) return "";
        String noComments = stripComments(sql);
        String collapsed = collapseWhitespace(noComments);
        return uppercaseOutsideLiterals(collapsed.trim());
    }

    static String collapseWhitespace(String sql) {
        StringBuilder sb = new StringBuilder(sql.length());
        boolean inWs = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                if (!inWs) {
                    sb.append(' ');
                    inWs = true;
                }
            } else {
                sb.append(c);
                inWs = false;
            }
        }
        return sb.toString();
    }

    static String stripComments(String sql) {
        StringBuilder sb = new StringBuilder(sql.length());
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1) < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    sb.append(c);
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && n == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }

            if (!inSingle && !inDouble) {
                if (c == '-' && n == '-') {
                    inLineComment = true;
                    i++;
                    continue;
                }
                if (c == '/' && n == '*') {
                    inBlockComment = true;
                    i++;
                    continue;
                }
            }

            if (!inDouble && c == '\'' ) {
                // SQL escaping: '' inside string
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
                inDouble = !inDouble;
                sb.append(c);
                continue;
            }

            sb.append(c);
        }

        return sb.toString();
    }

    static String uppercaseOutsideLiterals(String sql) {
        StringBuilder sb = new StringBuilder(sql.length());
        boolean inSingle = false;
        boolean inDouble = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1) < sql.length() ? sql.charAt(i + 1) : '\0';

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
                inDouble = !inDouble;
                sb.append(c);
                continue;
            }

            if (inSingle || inDouble) {
                sb.append(c);
            } else {
                sb.append(String.valueOf(c).toUpperCase(Locale.ROOT));
            }
        }
        return sb.toString();
    }
}
