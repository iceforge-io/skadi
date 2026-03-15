package org.iceforge.skadi.sqlgateway.trace;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TableauTraceLoggerTest {

    @Test
    void noExceptionWhenTraceDisabled() {
        TableauTraceLogger logger = new TableauTraceLogger("test123", false, null);
        // All methods should be no-ops or minimal when trace is off.
        logger.sessionStart(Map.of("user", "alice", "application_name", "Tableau Desktop"), "tableau");
        logger.statementParsed("SELECT id FROM orders");
        logger.queryStart("SELECT id FROM orders");
        logger.queryEnd("SELECT id FROM orders", 42, 15, "MISS");
        logger.queryError("SELECT id FROM orders", "XX000", "some error");
        logger.sessionEnd();
    }

    @Test
    void noExceptionWhenTraceEnabled() {
        TableauTraceLogger logger = new TableauTraceLogger("test456", true, null);
        logger.sessionStart(Map.of("user", "bob", "application_name", "Tableau Server"), "tableau");
        logger.statementParsed("SELECT * FROM sales");
        logger.queryStart("SELECT * FROM sales");
        logger.queryEnd("SELECT * FROM sales", 100, 200, "MISS");
        logger.queryError("SELECT * FROM sales", "42601", "syntax error");
        logger.sessionEnd();
    }

    @Test
    void corpusFileWrittenWhenPathConfigured(@TempDir Path tempDir) throws IOException {
        String sessionId = "corpustest1";
        TableauTraceLogger logger = new TableauTraceLogger(sessionId, true, tempDir.toString());

        logger.queryEnd("SELECT id FROM orders", 5, 10, "MISS");
        logger.queryEnd("SELECT id FROM orders", 5, 2, "HIT");

        Path corpusFile = tempDir.resolve(sessionId + ".jsonl");
        assertThat(corpusFile).exists();

        var lines = Files.readAllLines(corpusFile);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("\"fingerprint\"").contains("\"rows\":5").contains("\"cache_local\":\"MISS\"");
        assertThat(lines.get(1)).contains("\"cache_local\":\"HIT\"");
    }

    @Test
    void corpusFileNotWrittenWhenTraceDisabled(@TempDir Path tempDir) throws IOException {
        String sessionId = "corpustest2";
        TableauTraceLogger logger = new TableauTraceLogger(sessionId, false, null);
        logger.queryEnd("SELECT 1", 1, 5, "MISS");

        Path corpusFile = tempDir.resolve(sessionId + ".jsonl");
        assertThat(corpusFile).doesNotExist();
    }

    @Test
    void queryEndAlwaysEmittedEvenInTraceOffMode() {
        // queryEnd is the one event that always fires — just verify no exception
        TableauTraceLogger logger = new TableauTraceLogger("always", false, null);
        logger.queryEnd("SELECT count(*) FROM t", 1, 50, "SKIP");
    }
}
