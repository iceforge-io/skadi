package org.iceforge.skadi.sqlgateway.security;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B6 — Verifies that AuditLog emits correctly structured events on the {@code skadi.audit}
 * logger without leaking sensitive information.
 */
class AuditLogTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger auditLogger;

    @BeforeEach
    void attachAppender() {
        auditLogger = (Logger) LoggerFactory.getLogger("skadi.audit");
        appender = new ListAppender<>();
        appender.start();
        auditLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        auditLogger.detachAppender(appender);
    }

    // --- connect events ---

    @Test
    void connect_success_emits_audit_event() {
        AuditLog.connect("sess001", "alice", "tableau", true);

        List<ILoggingEvent> events = appender.list;
        assertThat(events).hasSize(1);
        String msg = events.get(0).getFormattedMessage();
        assertThat(msg).contains("event=audit_connect");
        assertThat(msg).contains("user=alice");
        assertThat(msg).contains("client=tableau");
        assertThat(msg).contains("session_id=sess001");
        assertThat(msg).contains("outcome=SUCCESS");
    }

    @Test
    void connect_failure_emits_denied_outcome() {
        AuditLog.connect("sess002", "baduser", "unknown", false);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("outcome=DENIED");
        assertThat(msg).contains("user=baduser");
    }

    @Test
    void connect_null_user_is_sanitised() {
        AuditLog.connect("sess003", null, null, false);
        String msg = appender.list.get(0).getFormattedMessage();
        // Null values become "-" placeholder — no NullPointerException
        assertThat(msg).contains("user=-");
        assertThat(msg).contains("client=-");
    }

    // --- query events ---

    @Test
    void query_event_contains_required_fields() {
        AuditLog.query("sess004", "qid001", "alice", "sales", "abc12345", "HIT", 42, 15);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("event=audit_query");
        assertThat(msg).contains("user=alice");
        assertThat(msg).contains("schema=sales");
        assertThat(msg).contains("fingerprint=abc12345");
        assertThat(msg).contains("cache=HIT");
        assertThat(msg).contains("rows=42");
        assertThat(msg).contains("latency_ms=15");
        assertThat(msg).contains("session_id=sess004");
        assertThat(msg).contains("query_id=qid001");
    }

    @Test
    void query_event_null_query_id_becomes_dash() {
        AuditLog.query("sess005", null, "bob", "public", "ff000000", "MISS", 0, 200);
        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("query_id=-");
    }

    // --- error events ---

    @Test
    void query_error_event_contains_sqlstate() {
        AuditLog.queryError("sess006", "qid002", "alice", "sales", "deadbeef", "XX000");

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("event=audit_error");
        assertThat(msg).contains("sqlstate=XX000");
        assertThat(msg).contains("fingerprint=deadbeef");
    }

    // --- log-injection protection ---

    @Test
    void newlines_in_user_are_sanitised() {
        // A malicious client could inject newlines to forge log entries.
        AuditLog.connect("sess007", "alice\nfake_event=audit_query", "tableau", true);
        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).doesNotContain("\n");
        assertThat(msg).contains("alice_fake_event=audit_query"); // underscore substitution
    }

    @Test
    void carriage_returns_in_client_are_sanitised() {
        AuditLog.connect("sess008", "alice", "tab\rleau", true);
        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).doesNotContain("\r");
    }

    // --- no SQL text in audit events ---

    @Test
    void audit_events_do_not_contain_sql_text() {
        // AuditLog must never accept or emit SQL text — only fingerprints.
        AuditLog.query("sess009", "qid003", "alice", "sales", "cafebabe", "MISS", 10, 50);
        String msg = appender.list.get(0).getFormattedMessage();
        // Verify the message only has a fingerprint, not the SQL itself.
        assertThat(msg).contains("fingerprint=cafebabe");
        assertThat(msg).doesNotContain("SELECT");
    }
}
