package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.List;
import java.util.Map;

public record SqlDialectBridgeResult(
        String translatedSql,
        List<SqlParam> translatedParams,
        String normalizedSqlForKey,
        List<SqlParam> normalizedParamsForKey,
        Map<String, String> diagnostics
) {
}
