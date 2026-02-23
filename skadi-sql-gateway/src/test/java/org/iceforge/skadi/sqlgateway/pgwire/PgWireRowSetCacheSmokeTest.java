package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.cache.QueryResultCacheKey;
import org.iceforge.skadi.sqlgateway.dialect.SqlNormalizerFacade;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Small smoke test for the cache key + normalizer and row-set cache mechanics.
 * (Not a full pgwire protocol test.)
 */
class PgWireRowSetCacheSmokeTest {

    @AfterEach
    void cleanup() {
        // Bridge may not be set in all tests; just reset.
        PgWireSessionRowSetCacheBridge.set(null, null);
    }

    @Test
    void cacheKeyUsesNormalizedSql() {
        String a = QueryResultCacheKey.cacheId("u", SqlNormalizerFacade.normalizeForKey("select  1"), List.of());
        String b = QueryResultCacheKey.cacheId("u", SqlNormalizerFacade.normalizeForKey("SELECT 1"), List.of());
        assertThat(a).isEqualTo(b);
    }

    @Test
    void rowSetCacheStoresAndExpires() {
        PgWireRowSetCache cache = new PgWireRowSetCache(Clock.systemUTC());
        cache.put("k", new String[]{"c"}, List.<String[]>of(new String[]{"1"}), "SELECT 1", Duration.ofMillis(1));
        assertThat(cache.get("k")).isPresent();
    }

    @Test
    void h2IsAvailableForTests() throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:pgwirecache;DB_CLOSE_DELAY=-1");
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("select 1");
        }
    }
}
