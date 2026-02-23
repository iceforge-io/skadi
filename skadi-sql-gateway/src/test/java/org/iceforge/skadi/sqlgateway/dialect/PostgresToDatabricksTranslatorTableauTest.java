package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresToDatabricksTranslatorTableauTest {

    private static final SqlDialectBridgeOptions OPTS = new SqlDialectBridgeOptions(
            SourceDialect.POSTGRES,
            ClientCompatibility.TABLEAU,
            true,
            true,
            true,
            true,
            true,
            true
    );

    @Test
    void does_not_rewrite_casts_inside_string_literals_or_comments() {
        String sql = "select '$1', /* $2 */ (\"t\".\"d\")::date -- (\"x\"::text)\n from \"sch\".\"t\" where c = '$3'";
        List<SqlParam> params = List.of(new SqlParam(1, null, 1));

        var tr = new PostgresToDatabricksTranslator().translate(sql, params, OPTS);

        // It *should* rewrite the real ::date cast and normalize identifier quotes,
        // but must not touch $1/$2/$3 inside literals/comments.
        assertThat(tr.sql()).contains("select '$1', /* $2 */");
        assertThat(tr.sql()).contains("-- (\"x\"::text)");
        assertThat(tr.sql()).contains("where c = '$3'");

        // And it should normalize identifiers + cast.
        assertThat(tr.sql()).contains("CAST((`t`.`d`) AS date)");
        assertThat(tr.sql()).contains("from `sch`.`t`");

        // Only $n markers outside literals/comments should be rewritten; here there are none.
        assertThat(tr.params()).isEmpty();
    }

    @Test
    void rewrites_dollarN_markers_outside_literals_and_comments_in_tableau_shape() {
        String sql = "select (\"t\".\"d\")::date from \"sch\".\"t\" where \"t\".\"x\" = $2 and \"t\".\"y\" = $1";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "one"),
                new SqlParam(2, null, "two")
        );

        var tr = new PostgresToDatabricksTranslator().translate(sql, params, OPTS);

        assertThat(tr.sql()).isEqualTo("select CAST((`t`.`d`) AS date) from `sch`.`t` where `t`.`x` = ? and `t`.`y` = ?");
        assertThat(tr.params()).extracting(SqlParam::value).containsExactly("two", "one");
        assertThat(tr.params()).extracting(SqlParam::index).containsExactly(1, 2);
    }
}
