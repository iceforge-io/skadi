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

    // --- spec-required equivalence pairs (ai/skadi-query-normalization.md) ---

    @Test
    void whitespaceVariantsAreEquivalent() {
        String a = SqlNormalizer.normalizeForKey("select * from risk");
        String b = SqlNormalizer.normalizeForKey(" SELECT  *  FROM risk ");
        assertThat(a).isEqualTo(b);
    }

    @Test
    void multilineQueryNormalizesToSingleLine() {
        String sql = "select cob_date,\nSUM(pnl)\nFROM risk";
        String norm = SqlNormalizer.normalizeForKey(sql);
        assertThat(norm).doesNotContain("\n");
        // Comma normalization removes surrounding spaces → no space between COB_DATE, and SUM(...)
        assertThat(norm).isEqualTo("SELECT COB_DATE,SUM(PNL) FROM RISK");
    }

    @Test
    void limitIsPreserved() {
        String a = SqlNormalizer.normalizeForKey("SELECT * FROM risk LIMIT 10");
        String b = SqlNormalizer.normalizeForKey("SELECT * FROM risk LIMIT 20");
        assertThat(a).isNotEqualTo(b);
        assertThat(a).contains("LIMIT 10");
        assertThat(b).contains("LIMIT 20");
    }

    @Test
    void orderByIsPreserved() {
        String norm = SqlNormalizer.normalizeForKey("SELECT * FROM risk ORDER BY cob_date");
        assertThat(norm).contains("ORDER BY");
    }

    @Test
    void distinctIsPreserved() {
        String norm = SqlNormalizer.normalizeForKey("SELECT DISTINCT cob_date FROM risk");
        assertThat(norm).contains("DISTINCT");
    }

    @Test
    void parameterPlaceholdersArePreserved() {
        String norm = SqlNormalizer.normalizeForKey(
                "SELECT cob_date, SUM(pnl) FROM mxl.gold_risk WHERE book = ?");
        assertThat(norm).contains("?");
    }

    @Test
    void differentPredicatesProduceDifferentKeys() {
        String a = SqlNormalizer.normalizeForKey("SELECT * FROM risk WHERE cob_date = ?");
        String b = SqlNormalizer.normalizeForKey("SELECT * FROM risk WHERE cob_date >= ?");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void doubleQuotedIdentifiersArePreserved() {
        String norm = SqlNormalizer.normalizeForKey("SELECT \"MyCol\" FROM t");
        assertThat(norm).contains("\"MyCol\"");
    }

    @Test
    void singleQuotedLiteralsArePreservedAsIs() {
        String norm = SqlNormalizer.normalizeForKey("SELECT * FROM t WHERE s = 'MiXeD CaSe'");
        assertThat(norm).contains("'MiXeD CaSe'");
    }
}
