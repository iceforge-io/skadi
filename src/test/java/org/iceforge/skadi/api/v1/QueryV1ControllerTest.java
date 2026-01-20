// language: java
package org.iceforge.skadi.api.v1;

import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryV1ControllerTest {

    @Mock
    QueryV1Registry registry;

    @Mock
    JdbcClientFactory jdbcClientFactory;

    @InjectMocks
    QueryV1Controller controller;

    @Captor
    ArgumentCaptor<QueryV1Models.SubmitQueryRequest> submitCaptor;

    @Test
    void submit_returnsAccepted_and_location() {
        QueryV1Models.SubmitQueryRequest req = mock(QueryV1Models.SubmitQueryRequest.class);
        QueryV1Registry.Entry entry = mock(QueryV1Registry.Entry.class);

        when(registry.create(any())).thenReturn(entry);
        when(entry.queryId()).thenReturn("q-123");
        when(entry.state()).thenReturn(QueryV1Models.State.QUEUED);

        ResponseEntity<QueryV1Models.SubmitQueryResponse> resp = controller.submit(req, null);

        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("q-123", resp.getBody().queryId());
        assertTrue(resp.getBody().resultsUrl().contains("/api/v1/queries/q-123/results"));

        verify(registry).create(submitCaptor.capture());
        assertSame(req, submitCaptor.getValue());
    }

    @Test
    void status_notFound_when_missing() {
        when(registry.get("missing")).thenReturn(Optional.empty());
        ResponseEntity<QueryV1Models.QueryStatusResponse> resp = controller.status("missing");
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void status_ok_returnsEntryDetails() {
        QueryV1Registry.Entry entry = mock(QueryV1Registry.Entry.class);
        when(registry.get("id")).thenReturn(Optional.of(entry));
        when(entry.queryId()).thenReturn("id");
        when(entry.state()).thenReturn(QueryV1Models.State.QUEUED);
        when(entry.rowsProduced()).thenReturn(0L);
        when(entry.bytesProduced()).thenReturn(0L);
        when(entry.startedAt()).thenReturn(Instant.now());
        when(entry.updatedAt()).thenReturn(Instant.now());
        when(entry.errorCode()).thenReturn(null);
        when(entry.message()).thenReturn(null);

        ResponseEntity<QueryV1Models.QueryStatusResponse> resp = controller.status("id");

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("id", resp.getBody().queryId());
        assertEquals(QueryV1Models.State.QUEUED, resp.getBody().state());
    }

    @Test
    void cancel_notFound_when_missing() {
        when(registry.get("missing")).thenReturn(Optional.empty());
        ResponseEntity<Void> resp = controller.cancel("missing");
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void cancel_requestsCancel_and_marksCanceled_ifQueued() {
        QueryV1Registry.Entry entry = mock(QueryV1Registry.Entry.class);
        when(registry.get("id")).thenReturn(Optional.of(entry));
        when(entry.state()).thenReturn(QueryV1Models.State.QUEUED);

        ResponseEntity<Void> resp = controller.cancel("id");

        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        verify(entry).requestCancel();
        verify(entry).markCanceled();
    }

    @Test
    void results_notFound_when_missing() {
        when(registry.get("x")).thenReturn(Optional.empty());
        ResponseEntity<StreamingResponseBody> resp = controller.results("x");
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void results_returnsGone_whenCanceledOrAlreadySucceeded() {
        QueryV1Registry.Entry canceled = mock(QueryV1Registry.Entry.class);
        when(registry.get("c")).thenReturn(Optional.of(canceled));
        when(canceled.state()).thenReturn(QueryV1Models.State.CANCELED);
        ResponseEntity<StreamingResponseBody> resp1 = controller.results("c");
        assertEquals(HttpStatus.GONE, resp1.getStatusCode());

        QueryV1Registry.Entry succeeded = mock(QueryV1Registry.Entry.class);
        when(registry.get("s")).thenReturn(Optional.of(succeeded));
        when(succeeded.state()).thenReturn(QueryV1Models.State.SUCCEEDED);
        ResponseEntity<StreamingResponseBody> resp2 = controller.results("s");
        assertEquals(HttpStatus.GONE, resp2.getStatusCode());
    }

    @Test
    void results_stream_missingSql_marksFailed_and_propagatesIOException() throws Exception {
        QueryV1Registry.Entry entry = mock(QueryV1Registry.Entry.class);
        when(registry.get("id")).thenReturn(Optional.of(entry));
        when(entry.state()).thenReturn(QueryV1Models.State.QUEUED);

        // prepare a request with blank SQL so controller triggers IllegalArgumentException inside streaming
        QueryV1Models.SubmitQueryRequest req = mock(QueryV1Models.SubmitQueryRequest.class);
        QueryV1Models.Jdbc jdbc = mock(QueryV1Models.Jdbc.class);
        when(req.jdbc()).thenReturn(jdbc);
        when(req.sql()).thenReturn("");
        when(entry.request()).thenReturn(req);

        // jdbcClientFactory should return a connection (will be closed)
        Connection conn = mock(Connection.class);
        when(jdbcClientFactory.openConnection(any())).thenReturn(conn);

        StreamingResponseBody body = controller.results("id").getBody();
        assertNotNull(body);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        assertThrows(IOException.class, () -> body.writeTo(baos));
        // ensure the controller attempted to mark running then mark failed with QUERY_FAILED
        verify(entry).markRunning();
        verify(entry).markFailed(eq("QUERY_FAILED"), contains("Missing sql"));
    }
}
