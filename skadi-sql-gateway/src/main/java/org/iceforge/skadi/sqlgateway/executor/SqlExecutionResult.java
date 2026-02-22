package org.iceforge.skadi.sqlgateway.executor;

import java.time.Duration;
import java.util.Map;

public record SqlExecutionResult(
        long rowsEmitted,
        String remoteQueryId,
        Duration elapsed,
        Map<String, String> diagnostics
) {
}

