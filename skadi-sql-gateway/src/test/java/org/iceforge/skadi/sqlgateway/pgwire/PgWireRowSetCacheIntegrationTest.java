package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.cache.QueryResultCacheKey;
import org.iceforge.skadi.sqlgateway.dialect.SqlNormalizerFacade;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PgWireRowSetCacheIntegrationTest {

    @Test
    void secondLookupIsCacheHitForSameNormalizedSql() {
        PgWireRowSetCache cache = new PgWireRowSetCache(Clock.systemUTC());

        String keyA = QueryResultCacheKey.cacheId(
                "user",
                SqlNormalizerFacade.normalizeForKey("select  1"),
                List.of()
        );
        String keyB = QueryResultCacheKey.cacheId(
                "user",
                SqlNormalizerFacade.normalizeForKey("SELECT 1"),
                List.of()
        );

        assertThat(keyA).isEqualTo(keyB);

        assertThat(cache.get(keyA)).isEmpty();

        cache.put(keyA, new String[]{"?column?"}, List.<String[]>of(new String[]{"1"}), "SELECT 1", Duration.ofMinutes(1));

        assertThat(cache.get(keyB)).isPresent();
        assertThat(cache.get(keyB).orElseThrow().rows()).hasSize(1);
    }

    @Test
    void ttlExpiresEntries() throws InterruptedException {
        PgWireRowSetCache cache = new PgWireRowSetCache(Clock.systemUTC());
        cache.put("k", new String[]{"c"}, List.<String[]>of(new String[]{"1"}), "SELECT 1", Duration.ofMillis(1));
        assertThat(cache.get("k")).isPresent();
        Thread.sleep(5);
        assertThat(cache.get("k")).isEmpty();
    }
}
