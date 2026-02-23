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

    @Test
    void normalizesSpacingAroundCommasParensAndEqualsOutsideLiterals() {
        String sql = "select  (  a  ,  b )  from t where x   =   1 and y='a = b'";
        String norm = SqlNormalizer.normalizeForKey(sql);
        assertThat(norm).isEqualTo("SELECT(A,B) FROM T WHERE X = 1 AND Y = 'a = b'");
    }
}
