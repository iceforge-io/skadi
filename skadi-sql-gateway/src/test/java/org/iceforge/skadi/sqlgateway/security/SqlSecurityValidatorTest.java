package org.iceforge.skadi.sqlgateway.security;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlSecurityValidatorTest {

    @Test
    void valid_select_passes() {
        assertThatCode(() -> SqlSecurityValidator.validate(
                "SELECT id, name FROM sales.orders LIMIT 100"))
                .doesNotThrowAnyException();
    }

    @Test
    void null_sql_passes() {
        assertThatCode(() -> SqlSecurityValidator.validate(null))
                .doesNotThrowAnyException();
    }

    @Test
    void blank_sql_passes() {
        assertThatCode(() -> SqlSecurityValidator.validate("   "))
                .doesNotThrowAnyException();
    }

    @Test
    void sql_with_null_byte_is_rejected() {
        String badSql = "SELECT 1\0; DROP TABLE orders; --";
        assertThatThrownBy(() -> SqlSecurityValidator.validate(badSql))
                .isInstanceOf(SqlSecurityValidator.SqlSecurityException.class)
                .hasMessageContaining("null byte");
    }

    @Test
    void sql_at_max_size_passes() throws Exception {
        String sql = "SELECT " + "x".repeat(SqlSecurityValidator.MAX_QUERY_BYTES - 7);
        assertThatCode(() -> SqlSecurityValidator.validate(sql))
                .doesNotThrowAnyException();
    }

    @Test
    void sql_exceeding_max_size_is_rejected() {
        String sql = "x".repeat(SqlSecurityValidator.MAX_QUERY_BYTES + 1);
        assertThatThrownBy(() -> SqlSecurityValidator.validate(sql))
                .isInstanceOf(SqlSecurityValidator.SqlSecurityException.class)
                .hasMessageContaining("maximum allowed size");
    }

    @Test
    void complex_tableau_style_query_passes() {
        // Tableau generates long queries with many columns and GROUP BYs.
        String sql = """
                SELECT
                    t0.region,
                    t0.category,
                    SUM(t0.sales) AS total_sales,
                    COUNT(DISTINCT t0.order_id) AS order_count
                FROM sales.orders t0
                WHERE t0.order_date >= '2021-01-01'
                  AND t0.order_date <  '2022-01-01'
                GROUP BY t0.region, t0.category
                ORDER BY total_sales DESC
                LIMIT 5000
                """;
        assertThatCode(() -> SqlSecurityValidator.validate(sql))
                .doesNotThrowAnyException();
    }

    @Test
    void exception_message_does_not_leak_sql_content() {
        // The error message must be safe to show the client without revealing internals.
        String badSql = "SELECT secret\0injected";
        assertThatThrownBy(() -> SqlSecurityValidator.validate(badSql))
                .isInstanceOf(SqlSecurityValidator.SqlSecurityException.class)
                .hasMessageNotContaining("secret")
                .hasMessageNotContaining("injected");
    }
}
