package org.iceforge.skadi.sqlgateway.cache;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryResultCacheKeyTest {

    @Test
    void cacheId_is_stable_for_same_inputs() {
        String a = QueryResultCacheKey.cacheId("user1", "SELECT 1", List.of(new SqlParam(1, null, "x")));
        String b = QueryResultCacheKey.cacheId("user1", "SELECT 1", List.of(new SqlParam(1, null, "x")));
        assertThat(a).isEqualTo(b);
    }

    @Test
    void cacheId_changes_when_user_scope_changes() {
        String a = QueryResultCacheKey.cacheId("u1", "SELECT 1", List.of());
        String b = QueryResultCacheKey.cacheId("u2", "SELECT 1", List.of());
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void cacheId_changes_when_param_value_changes() {
        String a = QueryResultCacheKey.cacheId("u", "SELECT ?", List.of(new SqlParam(1, null, "x")));
        String b = QueryResultCacheKey.cacheId("u", "SELECT ?", List.of(new SqlParam(1, null, "y")));
        assertThat(a).isNotEqualTo(b);
    }
}
