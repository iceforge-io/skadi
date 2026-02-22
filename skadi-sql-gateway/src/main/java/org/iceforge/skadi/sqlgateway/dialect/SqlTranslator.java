package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.List;

interface SqlTranslator {
    TranslateResult translate(String sql, List<SqlParam> params, SqlDialectBridgeOptions opts);

    record TranslateResult(String sql, List<SqlParam> params) {
    }
}

