package org.iceforge.skadi.sqlgateway.executor;

import org.junit.jupiter.api.Test;

import java.sql.SQLWarning;

import static org.assertj.core.api.Assertions.assertThat;

class DatabricksQueryIdExtractorTest {

    @Test
    void extractsQueryIdFromWarnings() {
        SQLWarning w = new SQLWarning("Query id: 01ef-abc_123");
        assertThat(DatabricksQueryIdExtractor.fromWarnings(w)).contains("01ef-abc_123");
    }

    @Test
    void returnsEmptyWhenNoMatch() {
        SQLWarning w = new SQLWarning("something else");
        assertThat(DatabricksQueryIdExtractor.fromWarnings(w)).isEmpty();
    }
}

