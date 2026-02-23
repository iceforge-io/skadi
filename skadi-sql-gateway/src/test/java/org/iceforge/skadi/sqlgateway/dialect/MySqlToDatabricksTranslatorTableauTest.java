package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MySqlToDatabricksTranslatorTableauTest {

    private static final SqlDialectBridgeOptions OPTS = new SqlDialectBridgeOptions(
            SourceDialect.MYSQL,
            ClientCompatibility.TABLEAU,
            true,
            true,
            true,
            true,
            true,
            true
    );

    @Test
    void offsetLimit_rewrite_swaps_last_two_params_when_both_are_qmarks() {
        String sql = "select * from t where a = ? offset ? limit ?";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "a"),
                new SqlParam(2, null, 10),
                new SqlParam(3, null, 5)
        );

        var tr = new MySqlToDatabricksTranslator().translate(sql, params, OPTS);

        assertThat(tr.sql()).isEqualTo("select * from t where a = ? LIMIT ? OFFSET ?");
        assertThat(tr.params()).extracting(SqlParam::value).containsExactly("a", 5, 10);
    }

    @Test
    void offsetLimit_rewrite_does_not_swap_when_offset_is_literal() {
        String sql = "select * from t where a = ? offset 10 limit ?";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "a"),
                new SqlParam(2, null, 5)
        );

        var tr = new MySqlToDatabricksTranslator().translate(sql, params, OPTS);

        assertThat(tr.sql()).isEqualTo("select * from t where a = ? LIMIT ? OFFSET 10");
        assertThat(tr.params()).extracting(SqlParam::value).containsExactly("a", 5);
    }

    @Test
    void offsetLimit_rewrite_does_not_swap_when_limit_is_literal() {
        String sql = "select * from t where a = ? offset ? limit 5";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "a"),
                new SqlParam(2, null, 10)
        );

        var tr = new MySqlToDatabricksTranslator().translate(sql, params, OPTS);

        assertThat(tr.sql()).isEqualTo("select * from t where a = ? LIMIT 5 OFFSET ?");
        assertThat(tr.params()).extracting(SqlParam::value).containsExactly("a", 10);
    }

    @Test
    void offsetLimit_rewrite_still_swaps_when_more_placeholders_exist_earlier_in_query() {
        String sql = "select * from t where a = ? and b = ? offset ? limit ?";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "a"),
                new SqlParam(2, null, "b"),
                new SqlParam(3, null, 10),
                new SqlParam(4, null, 5)
        );

        var tr = new MySqlToDatabricksTranslator().translate(sql, params, OPTS);

        assertThat(tr.sql()).isEqualTo("select * from t where a = ? and b = ? LIMIT ? OFFSET ?");
        assertThat(tr.params()).extracting(SqlParam::value).containsExactly("a", "b", 5, 10);
    }
}

