package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bridges source SQL dialects (pg/mysql) into a Databricks-friendly SQL form.
 *
 * <p>MVP: focuses on Tableau/JDBC compatibility and parameter marker preservation.
 */
public final class SqlDialectBridge {

    private final SqlDialectBridgeOptions opts;
    private final SqlTranslator translator;

    public SqlDialectBridge(SqlDialectBridgeOptions opts) {
        this.opts = Objects.requireNonNull(opts, "opts");
        this.translator = switch (opts.sourceDialect()) {
            case POSTGRES -> new PostgresToDatabricksTranslator();
            case MYSQL -> new MySqlToDatabricksTranslator();
        };
    }

    public SqlDialectBridgeResult bridge(String sql, List<SqlParam> params) {
        Map<String, String> diag = new HashMap<>();

        String inSql = sql == null ? "" : sql;
        List<SqlParam> inParams = params == null ? List.of() : params;

        String translatedSql = inSql;
        List<SqlParam> translatedParams = inParams;

        if (opts.translateEnabled()) {
            SqlTranslator.TranslateResult tr = translator.translate(inSql, inParams, opts);
            translatedSql = tr.sql();
            translatedParams = tr.params();
        }

        String normalizedSql = opts.normalizeForCacheKeys() ? SqlNormalizer.normalizeForKey(translatedSql) : translatedSql;

        // Params are already sequential after marker rewrite; for keys we keep them as-is.
        List<SqlParam> normalizedParams = translatedParams;

        return new SqlDialectBridgeResult(translatedSql, translatedParams, normalizedSql, normalizedParams, diag);
    }
}

