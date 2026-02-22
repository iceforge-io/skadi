package org.iceforge.skadi.sqlgateway.dialect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SqlNormalizerTest {

    @Test
    void normalizesWhitespaceAndStripsCommentsAndUppercases() {
        String sql = "select  1  -- hi\n  from  t /*block*/ where c = 'MiXeD'";
        String norm = SqlNormalizer.normalizeForKey(sql);
        assertThat(norm).isEqualTo("SELECT 1 FROM T WHERE C = 'MiXeD'");
    }
}

