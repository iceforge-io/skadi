package org.iceforge.skadi.sqlgateway.executor;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public record SqlExecutionRequest(
        String sql,
        List<SqlParam> params,
        Duration timeout,
        int fetchSize,
        int batchRows,
        Map<String, String> tags
) {
}

