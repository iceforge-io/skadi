package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlDialectBridgeTableauTest {

    @Test
    void postgres_cast_and_quoted_identifiers_are_mapped_for_databricks() {
        var opts = new SqlDialectBridgeOptions(
                SourceDialect.POSTGRES,
                ClientCompatibility.TABLEAU,
                true,
                true,
                true,
                true,
                true,
                true
        );
        var bridge = new SqlDialectBridge(opts);

        String sql = "select (\"t\".\"d\")::date as \"D\" from \"sch\".\"t\" where \"t\".\"x\" = $1";
        List<SqlParam> params = List.of(new SqlParam(1, null, 123));

        var res = bridge.bridge(sql, params);

        assertThat(res.translatedSql())
                .isEqualTo("select CAST((`t`.`d`) AS date) as `D` from `sch`.`t` where `t`.`x` = ?");
        assertThat(res.translatedParams()).extracting(SqlParam::index).containsExactly(1);
        assertThat(res.translatedParams()).extracting(SqlParam::value).containsExactly(123);

        // Cache-key SQL should be stable and independent of cosmetic formatting.
        assertThat(res.normalizedSqlForKey())
                .isEqualTo("SELECT CAST((`T`.`D`) AS DATE) AS `D` FROM `SCH`.`T` WHERE `T`.`X` = ?");
    }

    @Test
    void mysql_offset_limit_is_canonicalized_and_markers_preserved() {
        var opts = new SqlDialectBridgeOptions(
                SourceDialect.MYSQL,
                ClientCompatibility.TABLEAU,
                true,
                true,
                true,
                true,
                true,
                true
        );

        var bridge = new SqlDialectBridge(opts);

        String sql = "select `a` from `t` where `x` = ? offset ? limit ?";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "val"),
                new SqlParam(2, null, 10),
                new SqlParam(3, null, 5)
        );

        var res = bridge.bridge(sql, params);

        assertThat(res.translatedSql()).isEqualTo("select `a` from `t` where `x` = ? LIMIT ? OFFSET ?");
        // OFFSET/LIMIT were rewritten, so the corresponding bind values must be swapped (limit then offset).
        assertThat(res.translatedParams()).extracting(SqlParam::value).containsExactly("val", 5, 10);

        assertThat(res.normalizedSqlForKey()).isEqualTo("SELECT `A` FROM `T` WHERE `X` = ? LIMIT ? OFFSET ?");
    }
}
