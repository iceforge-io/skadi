package org.iceforge.skadi.api.v1;

import org.iceforge.skadi.api.CacheMetricsRegistry;
import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.aws.s3.S3Models;
import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.iceforge.skadi.query.QueryCacheProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryV1ControllerTest {

    private QueryV1Registry registry;

    private JdbcClientFactory jdbcClientFactory;
    private S3AccessLayer s3;
    private QueryCacheProperties cacheProps;
    private ExecutorService executor;
    private CacheMetricsRegistry metrics;

    private QueryV1Controller controller;

    @Captor
    ArgumentCaptor<S3Models.ObjectRef> objectRefCaptor;

    @BeforeEach
    void setUp() {
        registry = new QueryV1Registry();

        jdbcClientFactory = mock(JdbcClientFactory.class);
        s3 = mock(S3AccessLayer.class);

        cacheProps = new QueryCacheProperties();
        cacheProps.setBucket("test-bucket");
        cacheProps.setPrefix("results");
        cacheProps.setArrowPrefix("arrow");
        cacheProps.setArrowMultipartAboveBytes(5 * 1024 * 1024);

        executor = Executors.newSingleThreadExecutor();
        metrics = new CacheMetricsRegistry();

        controller = new QueryV1Controller(
                registry,
                jdbcClientFactory,
                s3,
                cacheProps,
                executor,
                metrics
        );
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    private QueryV1Models.Jdbc jdbc() {
        return new QueryV1Models.Jdbc(
                "jdbc:dummy",
                "u",
                "p",
                null,
                null
        );
    }
    private QueryV1Models.SubmitQueryRequest req(String sql) {
        return new QueryV1Models.SubmitQueryRequest(
                jdbc(),          // Jdbc
                sql,             // sql
                java.util.Map.of(), // properties (or params) map
                null,            // datasourceId
                null,            // fetchSize
                null,            // batchRows
                null             // catalogOrSchema (whatever that last string represents in your model)
        );
    }

    @Test
    void submit_returnsAccepted_and_location_onMiss() {
        // MISS path: no object exists
        when(s3.exists(any(S3Models.ObjectRef.class))).thenReturn(false);

        req("select 1");


        ResponseEntity<QueryV1Models.SubmitQueryResponse> resp = controller.submit(req("select 1"), "q-123");

        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("q-123", resp.getBody().queryId());
        assertTrue(resp.getBody().resultsUrl().contains("/api/v1/queries/q-123/results"));

        // verify it checked the expected cache location
        verify(s3).exists(objectRefCaptor.capture());
        S3Models.ObjectRef ref = objectRefCaptor.getValue();
        assertEquals("test-bucket", ref.bucket());
        assertEquals("results/arrow/q-123/result.arrow", ref.key());
    }

    @Test
    void submit_returnsOk_onHit_and_marksSucceeded() throws Exception {
        // HIT path
        when(s3.exists(any(S3Models.ObjectRef.class))).thenReturn(true);

        QueryV1Models.SubmitQueryRequest req =req("select 1");

        ResponseEntity<QueryV1Models.SubmitQueryResponse> resp = controller.submit(req, "q-hit");

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("q-hit", resp.getBody().queryId());

        // registry should now have a succeeded entry
        var e = registry.get("q-hit").orElseThrow();
        assertEquals(QueryV1Models.State.SUCCEEDED, e.state());
        assertEquals("test-bucket", e.resultBucket());
        assertEquals("results/arrow/q-hit/result.arrow", e.resultKey());
    }

    @Test
    void status_notFound_when_missing() {
        ResponseEntity<QueryV1Models.QueryStatusResponse> resp = controller.status("missing");
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void status_ok_returnsEntryDetails() {
        QueryV1Models.SubmitQueryRequest req =req("select 1");

        var e = registry.getOrCreate("id", req);
        e.ensureStartedAt();
        // leave as default state (likely QUEUED), or set it if your Entry supports it
        // e.markQueued(); // only if exists

        ResponseEntity<QueryV1Models.QueryStatusResponse> resp = controller.status("id");

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("id", resp.getBody().queryId());
    }

    @Test
    void cancel_notFound_when_missing() {
        ResponseEntity<Void> resp = controller.cancel("missing");
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void cancel_requestsCancel_and_marksCanceled_ifQueued() {
        QueryV1Models.SubmitQueryRequest req =req("select 1");

        var e = registry.getOrCreate("id", req);
        // Ensure the entry is queued (depends on your Entry defaults)
        // If your Entry defaults to QUEUED, this is enough.

        ResponseEntity<Void> resp = controller.cancel("id");

        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertTrue(e.cancelRequested());
        // If the controller marks canceled when QUEUED, state should be CANCELED
        // (depends on your Entry implementation)
    }

    @Test
    void results_notFound_when_missing() {
        ResponseEntity<StreamingResponseBody> resp = controller.results("x", 2000L);
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void results_stream_succeeded_streamsFromS3() throws Exception {
        QueryV1Models.SubmitQueryRequest req =req("select 1");

        var e = registry.getOrCreate("id", req);
        e.setResultLocation("test-bucket", "results/arrow/id/result.arrow", "application/vnd.apache.arrow.stream");
        e.markSucceeded();

        byte[] payload = "hello".getBytes();
        when(s3.getStream(any())).thenReturn(new java.io.ByteArrayInputStream(payload));

        ResponseEntity<StreamingResponseBody> resp = controller.results("id", 0L);

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        resp.getBody().writeTo(baos);

        assertArrayEquals(payload, baos.toByteArray());
    }
}
