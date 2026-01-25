package org.iceforge.skadi.api.v1;

        import com.fasterxml.jackson.databind.ObjectMapper;
        import org.apache.arrow.memory.BufferAllocator;
        import org.apache.arrow.memory.RootAllocator;
        import org.iceforge.skadi.arrow.JdbcArrowStreamer;
        import org.iceforge.skadi.aws.s3.S3AccessLayer;
        import org.iceforge.skadi.aws.s3.S3Models;
        import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
        import org.iceforge.skadi.query.QueryModels;
        import org.iceforge.skadi.query.QueryCacheProperties;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;
        import org.springframework.http.HttpHeaders;
        import org.springframework.http.HttpStatus;
        import org.springframework.http.MediaType;
        import org.springframework.http.ResponseEntity;
        import org.springframework.web.bind.annotation.*;
        import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

        import java.io.*;
        import java.nio.file.Files;
        import java.nio.file.Path;
        import java.sql.Connection;
        import java.time.Duration;
        import java.time.Instant;
        import java.util.Objects;
        import java.util.concurrent.ExecutorService;
        import java.util.concurrent.TimeUnit;

        @RestController
        @RequestMapping("/api/v1/queries")
        public class QueryV1Controller {
            private static final Logger log = LoggerFactory.getLogger(QueryV1Controller.class);

            private final QueryV1Registry registry;
            private final JdbcClientFactory jdbcClientFactory;
            private final S3AccessLayer s3;
            private final QueryCacheProperties cacheProps;
            private final ExecutorService queryExecutor;

            public QueryV1Controller(QueryV1Registry registry,
                                     JdbcClientFactory jdbcClientFactory,
                                     S3AccessLayer s3,
                                     QueryCacheProperties cacheProps,
                                     ExecutorService queryExecutor) {
                this.registry = Objects.requireNonNull(registry);
                this.jdbcClientFactory = Objects.requireNonNull(jdbcClientFactory);
                this.s3 = Objects.requireNonNull(s3);
                this.cacheProps = Objects.requireNonNull(cacheProps);
                this.queryExecutor = Objects.requireNonNull(queryExecutor);
            }

            @PostMapping
            public ResponseEntity<QueryV1Models.SubmitQueryResponse> submit(
                    @RequestBody QueryV1Models.SubmitQueryRequest req,
                    @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {

                QueryV1Registry.Entry e = registry.create(req);
                startMaterialization(e);

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

                return ResponseEntity.ok(QueryV1Models.QueryStatusResponse.from(e));
            }

            @GetMapping("/{queryId}/results")
            public ResponseEntity<StreamingResponseBody> results(@PathVariable String queryId,
                                                                 @RequestParam(value = "waitMs", required = false) Long waitMs) {
                QueryV1Registry.Entry e = registry.get(queryId).orElse(null);
                if (e == null) return ResponseEntity.notFound().build();

                if ((e.state() == QueryV1Models.State.QUEUED || e.state() == QueryV1Models.State.RUNNING)
                        && waitMs != null && waitMs > 0) {
                    try {
                        e.completion().get(waitMs, TimeUnit.MILLISECONDS);
                    } catch (Exception ignored) {
                    }
                }

                if (e.state() == QueryV1Models.State.CANCELED) return ResponseEntity.status(HttpStatus.GONE).build();
                if (e.state() == QueryV1Models.State.FAILED) {
                    QueryV1Models.QueryStatusResponse status =
                            QueryV1Models.QueryStatusResponse.from(e);

                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(out -> {
                                new ObjectMapper().writeValue(out, status);
                            });
                }

                if (e.state() != QueryV1Models.State.SUCCEEDED) return ResponseEntity.status(HttpStatus.CONFLICT).build();

                if (e.resultBucket() == null || e.resultKey() == null) {
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                }

                StreamingResponseBody body = out -> {
                    S3Models.ObjectRef ref = new S3Models.ObjectRef(e.resultBucket(), e.resultKey());
                    try (InputStream in = s3.getStream(ref)) {
                        in.transferTo(out);
                    }
                };

                String ct = Objects.requireNonNullElse(e.resultContentType(), "application/vnd.apache.arrow.stream");
                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_TYPE, ct)
                        .header("Skadi-Query-Id", e.queryId())
                        .body(body);
            }

            @DeleteMapping("/{queryId}")
            public ResponseEntity<Void> cancel(@PathVariable String queryId) {
                QueryV1Registry.Entry e = registry.get(queryId).orElse(null);
                if (e == null) return ResponseEntity.notFound().build();
                e.requestCancel();
                if (e.state() == QueryV1Models.State.QUEUED) {
                    e.markCanceled();
                }
                return ResponseEntity.accepted().build();
            }

            private void startMaterialization(QueryV1Registry.Entry e) {
                queryExecutor.submit(() -> {
                    e.markRunning();

                    Path tmp = null;
                    String bucket = cacheProps.getBucket();
                    String key = cacheProps.getPrefix() + "/" + cacheProps.getArrowPrefix() + "/" + e.queryId() + "/result.arrow";
                    S3Models.ObjectRef ref = new S3Models.ObjectRef(bucket, key);

                    try (BufferAllocator allocator = new RootAllocator();
                         Connection conn = openConnection(e.request())) {

                        String sql = Objects.requireNonNullElse(e.request().sql(), "");
                        if (sql.isBlank()) throw new IllegalArgumentException("Missing sql");

                        tmp = Files.createTempFile("skadi-" + e.queryId() + "-", ".arrow");
                        try (OutputStream fileOut = Files.newOutputStream(tmp);
                             CountingOutputStream counting = new CountingOutputStream(fileOut, e)) {

                            int fetchSize = 1_000;
                            int batchRows = 4_096;
                            long rows = JdbcArrowStreamer.stream(
                                    conn,
                                    sql,
                                    fetchSize,
                                    batchRows,
                                    allocator,
                                    counting,
                                    e::cancelRequested,
                                    n -> e.addRows(n)
                            );
                            if (rows > 0 && e.rowsProduced() == 0) {
                                e.addRows(rows);
                            }
                        }

                        if (e.cancelRequested()) {
                            e.markCanceled();
                            safeDelete(ref);
                            return;
                        }

                        long len = Files.size(tmp);
                        try (InputStream in = Files.newInputStream(tmp)) {
                            if (len >= cacheProps.getArrowMultipartAboveBytes()) {
                                s3.multipartUpload(ref, in, len, "application/vnd.apache.arrow.stream", java.util.Map.of());
                            } else {
                                s3.putStream(ref, in, len, "application/vnd.apache.arrow.stream", java.util.Map.of());
                            }
                        }

                        e.setResultLocation(bucket, key, "application/vnd.apache.arrow.stream");
                        e.markSucceeded();
                    } catch (Exception ex) {
                        if (e.cancelRequested()) {
                            e.markCanceled();
                        } else {
                            e.markFailed("QUERY_FAILED", ex);
                            log.error("Query materialization failed: queryId={}", e.queryId(), ex); // <-- THIS is what you’re missing
                        }
                        safeDelete(ref);
                    } finally {
                        if (tmp != null) {
                            try { Files.deleteIfExists(tmp); } catch (Exception ignore) { }
                        }
                    }
                });
            }

            private void safeDelete(S3Models.ObjectRef ref) {
                try {
                    if (s3.exists(ref)) {
                        s3.delete(ref);
                    }
                } catch (Exception ignored) {
                }
            }

            private Connection openConnection(QueryV1Models.SubmitQueryRequest req) throws Exception {
                QueryV1Models.Jdbc jdbc = Objects.requireNonNull(req.jdbc(), "jdbc");
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