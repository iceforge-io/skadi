package org.iceforge.skadi.api.v1;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.iceforge.skadi.arrow.JdbcArrowStreamer;
import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.iceforge.skadi.query.QueryModels;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Option A: HTTP streaming + Arrow IPC.
 *
 * This is a minimal reference implementation that:
 *  - stores the submitted query request in-memory (v1)
 *  - streams results on-demand as application/vnd.apache.arrow.stream
 *  - supports cancellation via DELETE
 */
@RestController
@RequestMapping("/api/v1/queries")
public class QueryV1Controller {

    private final QueryV1Registry registry;
    private final JdbcClientFactory jdbcClientFactory;

    public QueryV1Controller(QueryV1Registry registry, JdbcClientFactory jdbcClientFactory) {
        this.registry = Objects.requireNonNull(registry);
        this.jdbcClientFactory = Objects.requireNonNull(jdbcClientFactory);
    }

    @PostMapping
    public ResponseEntity<QueryV1Models.SubmitQueryResponse> submit(
            @RequestBody QueryV1Models.SubmitQueryRequest req,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {

        // v1 ref impl: ignore idempotencyKey (wire it in once you add persistence)
        QueryV1Registry.Entry e = registry.create(req);

        Instant expiresAt = Instant.now().plus(Duration.ofHours(1));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(
                new QueryV1Models.SubmitQueryResponse(
                        e.queryId(),
                        e.state(),
                        "/api/v1/queries/" + e.queryId() + "/results",
                        expiresAt
                )
        );
    }

    @GetMapping("/{queryId}/status")
    public ResponseEntity<QueryV1Models.QueryStatusResponse> status(@PathVariable String queryId) {
        QueryV1Registry.Entry e = registry.get(queryId).orElse(null);
        if (e == null) return ResponseEntity.notFound().build();

        return ResponseEntity.ok(new QueryV1Models.QueryStatusResponse(
                e.queryId(),
                e.state(),
                e.rowsProduced(),
                e.bytesProduced(),
                e.startedAt(),
                e.updatedAt(),
                e.errorCode(),
                e.message(),
                null
        ));
    }

    @GetMapping("/{queryId}/results")
    public ResponseEntity<StreamingResponseBody> results(@PathVariable String queryId) {
        QueryV1Registry.Entry e = registry.get(queryId).orElse(null);
        if (e == null) return ResponseEntity.notFound().build();
        if (e.state() == QueryV1Models.State.CANCELED) return ResponseEntity.status(HttpStatus.GONE).build();

        // v1 ref impl: only allow streaming once unless you add caching.
        if (e.state() == QueryV1Models.State.SUCCEEDED) {
            return ResponseEntity.status(HttpStatus.GONE).build();
        }

        StreamingResponseBody body = out -> {
            e.markRunning();

            // Count bytes written (compressed encodings can be layered later)
            CountingOutputStream counting = new CountingOutputStream(out, e);

            try (BufferAllocator allocator = new RootAllocator();
                 Connection conn = openConnection(e.request())) {

                String sql = Objects.requireNonNullElse(e.request().sql(), "");
                if (sql.isBlank()) throw new IllegalArgumentException("Missing sql");

                // Reasonable v1 defaults; lift into config later
                int fetchSize = 1_000;
                int batchRows = 4_096;

                JdbcArrowStreamer.stream(conn, sql, fetchSize, batchRows, allocator, counting, e::cancelRequested);

                if (e.cancelRequested()) {
                    e.markCanceled();
                } else {
                    e.markSucceeded();
                }
            } catch (Exception ex) {
                if (e.cancelRequested()) {
                    e.markCanceled();
                } else {
                    e.markFailed("QUERY_FAILED", ex.getMessage());
                }
                // Let Spring propagate the exception so the client sees the failure.
                if (ex instanceof IOException ioe) throw ioe;
                throw new IOException("Arrow stream failed", ex);
            }
        };

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, "application/vnd.apache.arrow.stream")
                .header("Skadi-Query-Id", e.queryId())
                .body(body);
    }

    @DeleteMapping("/{queryId}")
    public ResponseEntity<Void> cancel(@PathVariable String queryId) {
        QueryV1Registry.Entry e = registry.get(queryId).orElse(null);
        if (e == null) return ResponseEntity.notFound().build();
        e.requestCancel();
        // If not currently running, mark as canceled immediately.
        if (e.state() == QueryV1Models.State.QUEUED) {
            e.markCanceled();
        }
        return ResponseEntity.accepted().build();
    }

    private Connection openConnection(QueryV1Models.SubmitQueryRequest req) throws Exception {
        QueryV1Models.Jdbc jdbc = Objects.requireNonNull(req.jdbc(), "jdbc");
        // Reuse existing JdbcClientFactory SPI by mapping into the existing model.
        QueryModels.QueryRequest.Jdbc mapped = new QueryModels.QueryRequest.Jdbc(
                jdbc.jdbcUrl(),
                jdbc.username(),
                jdbc.password(),
                req.sql(),
                null,
                jdbc.datasourceId(),
                jdbc.properties()
        );
        return jdbcClientFactory.openConnection(mapped);
    }

    private static final class CountingOutputStream extends FilterOutputStream {
        private final QueryV1Registry.Entry entry;
        private long bytes = 0;

        private CountingOutputStream(OutputStream out, QueryV1Registry.Entry entry) {
            super(out);
            this.entry = entry;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            bytes++;
            entry.addBytes(1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            bytes += len;
            entry.addBytes(len);
        }
    }
}
