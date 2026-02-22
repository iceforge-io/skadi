package org.iceforge.skadi.sqlgateway.dialect;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ParameterMarkerRewriterTest {

    @Test
    void rewritesDollarNOutsideLiteralsAndComments() {
        String sql = "select $1, '$2', -- $3\n $2";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "a"),
                new SqlParam(2, null, "b")
        );

        var rr = ParameterMarkerRewriter.rewriteToJdbcQMarks(sql, params, SourceDialect.POSTGRES);

        assertThat(rr.sql()).isEqualTo("select ?, '$2', -- $3\n ?");
        assertThat(rr.params()).extracting(SqlParam::value).containsExactly("a", "b");
        assertThat(rr.params()).extracting(SqlParam::index).containsExactly(1, 2);
    }

    @Test
    void supportsOutOfOrderAndRepeatedMarkers() {
        String sql = "select $2, $1, $2";
        List<SqlParam> params = List.of(
                new SqlParam(1, null, "one"),
                new SqlParam(2, null, "two")
        );

        var rr = ParameterMarkerRewriter.rewriteToJdbcQMarks(sql, params, SourceDialect.POSTGRES);

        assertThat(rr.sql()).isEqualTo("select ?, ?, ?");
        assertThat(rr.params()).extracting(SqlParam::value).containsExactly("two", "one", "two");
        assertThat(rr.params()).extracting(SqlParam::index).containsExactly(1, 2, 3);
    }
}

