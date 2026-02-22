package org.iceforge.skadi.sqlgateway.executor;

import java.sql.SQLWarning;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class DatabricksQueryIdExtractor {
    private static final Pattern[] PATTERNS = new Pattern[]{
            Pattern.compile("(?i)\\bquery\\s*id\\b\\s*[:=]\\s*([A-Za-z0-9_-]+)"),
            Pattern.compile("(?i)\\bstatement\\s*id\\b\\s*[:=]\\s*([A-Za-z0-9_-]+)"),
            Pattern.compile("(?i)\\bquery\\b\\s*([A-Za-z0-9_-]{8,})")
    };

    private DatabricksQueryIdExtractor() {
    }

    static Optional<String> fromWarnings(SQLWarning warning) {
        SQLWarning w = warning;
        while (w != null) {
            String msg = w.getMessage();
            if (msg != null) {
                for (Pattern p : PATTERNS) {
                    Matcher m = p.matcher(msg);
                    if (m.find()) {
                        return Optional.ofNullable(m.group(1));
                    }
                }
            }
            w = w.getNextWarning();
        }
        return Optional.empty();
    }
}
