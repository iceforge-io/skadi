package com.dkay229.skadi.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

/**
 * Service to write a JDBC ResultSet into S3 as chunked objects with a manifest.
 * <br>
 * Example usage:
 * <pre>
 * var plan = new ResultSetToS3ChunkWriter.S3WritePlan(
 *     "my-bucket",
 *     "skadi/results",
 *     "run-" + System.currentTimeMillis()
 * );
 *
 * var opt = ResultSetToS3ChunkWriter.StreamOptions.defaults();
 *
 * S3ResultSetRef ref = writer.write(conn, "select * from big_table", plan, opt);
 *
 * // ref.manifestKey() is the stable handle for consumers
 * System.out.println(ref);
 * </pre>
 */
@Service
public class ResultSetToS3ChunkWriter {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetToS3ChunkWriter.class);
    private final S3AccessLayer s3;

    @Autowired
    public ResultSetToS3ChunkWriter(AwsSdkS3AccessLayer s3) {
        this.s3 = Objects.requireNonNull(s3);
    }

    public S3ResultSetRef write(
            Connection conn,
            String sql,
            S3WritePlan plan,
            StreamOptions opt
    ) throws Exception {
        logger.info("Starting ResultSet->S3 chunked write: runId={}, bucket={}, prefix={}",
                plan.runId(), plan.bucket(), plan.prefix()
        );
        Objects.requireNonNull(conn);
        Objects.requireNonNull(sql);
        Objects.requireNonNull(plan);
        Objects.requireNonNull(opt);

        // Backpressure: (1) bounded queue of chunks, (2) bounded bytes in-flight.
        BlockingQueue<SealedChunk> queue = new ArrayBlockingQueue<>(opt.maxInFlightChunks());
        Semaphore inflightBytes = new Semaphore(opt.maxInFlightBytes());

        ExecutorService uploadPool = Executors.newFixedThreadPool(opt.uploadThreads(), r -> {
            Thread t = new Thread(r, "skadi-s3-upload");
            t.setDaemon(true);
            return t;
        });

        AtomicReference<Throwable> firstError = new AtomicReference<>(null);
        List<ChunkDescriptor> chunks = Collections.synchronizedList(new ArrayList<>());

        // Start N uploader workers
        List<Future<?>> workers = new ArrayList<>();
        for (int i = 0; i < opt.uploadThreads(); i++) {
            workers.add(uploadPool.submit(() ->
                    uploaderLoop(plan, opt, queue, inflightBytes, chunks, firstError)
            ));
        }

        long totalRows = 0;
        long totalUncompressedBytes = 0;
        int part = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(opt.jdbcFetchSize());

            // Some drivers (e.g., Postgres) require this for streaming:
            // conn.setAutoCommit(false);

            try (ResultSet rs = ps.executeQuery()) {
                RowEncoder encoder = opt.rowEncoder();

                ChunkBuilder builder = new ChunkBuilder(opt.targetChunkBytes());
                logger.info("Processing ResultSet and building chunks...");
                while (rs.next()) {
                    encoder.writeRow(rs, builder.out());
                    totalRows++;

                    if (builder.size() >= opt.targetChunkBytes()) {
                        part++;
                        SealedChunk sealed = builder.seal(
                                plan.chunkRef(part, encoder.fileExtension(opt.compress())),
                                encoder.contentType(opt.compress()),
                                opt.compress(),
                                part
                        );
                        builder.reset();

                        inflightBytes.acquire(sealed.payload.length);
                        queue.put(sealed);

                        totalUncompressedBytes += sealed.uncompressedBytes;
                    }
                }
                logger.info("wrote {} rows into {} part", totalRows, part);
                // flush last chunk
                if (builder.size() > 0) {
                    part++;
                    SealedChunk sealed = builder.seal(
                            plan.chunkRef(part, encoder.fileExtension(opt.compress())),
                            encoder.contentType(opt.compress()),
                            opt.compress(),
                            part
                    );
                    builder.reset();

                    inflightBytes.acquire(sealed.payload.length);
                    queue.put(sealed);

                    totalUncompressedBytes += sealed.uncompressedBytes;
                }
            }
        } catch (Throwable t) {
            firstError.compareAndSet(null, t);
        } finally {
            // Stop workers
            logger.info("Signaling upload workers to stop...");
            for (int i = 0; i < opt.uploadThreads(); i++) queue.offer(SealedChunk.POISON);

            logger.info("Waiting for upload workers to finish...");
            for (Future<?> f : workers) {
                try {
                    f.get();
                } catch (Exception e) {
                    firstError.compareAndSet(null, e);
                }
            }
            logger.info("All upload workers finished.");
            uploadPool.shutdownNow();
        }

        if (firstError.get() != null) {
            throw new RuntimeException("ResultSet->S3 chunked write failed", firstError.get());
        }

        chunks.sort(Comparator.comparingInt(ChunkDescriptor::partNumber));

        Manifest manifest = new Manifest(
                plan.runId(),
                Instant.now().toString(),
                sql,
                totalRows,
                totalUncompressedBytes,
                chunks
        );

        byte[] manifestBytes = opt.manifestSerializer().serialize(manifest);

        Map<String, String> md = new HashMap<>();
        md.put("skadi-kind", "resultset-manifest");
        md.put("skadi-run-id", plan.runId());
        md.put("skadi-total-rows", String.valueOf(totalRows));
        md.put("skadi-chunk-count", String.valueOf(chunks.size()));
        logger.info("Uploading manifest ({} bytes)...", manifestBytes.length);
        s3.putBytes(plan.manifestRef(), manifestBytes, "application/json", md);

        return new S3ResultSetRef(plan.bucket(), plan.manifestRef().key(), plan.runId(), totalRows, chunks.size());
    }

    private void uploaderLoop(
            S3WritePlan plan,
            StreamOptions opt,
            BlockingQueue<SealedChunk> queue,
            Semaphore inflightBytes,
            List<ChunkDescriptor> out,
            AtomicReference<Throwable> firstError
    ) {
        while (firstError.get() == null) {
            SealedChunk chunk;
            try {
                chunk = queue.take();
                logger.info("Uploader picked up chunk part {} ({} bytes)", chunk.partNumber, chunk.payload.length);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            if (chunk == SealedChunk.POISON) {
                logger.info("Uploader received POISON, exiting");
                return;
            }

            try {
                Map<String, String> md = new HashMap<>();
                md.put("skadi-kind", "resultset-chunk");
                md.put("skadi-run-id", plan.runId());
                md.put("skadi-part", String.valueOf(chunk.partNumber));
                md.put("skadi-rows", String.valueOf(chunk.rowCount));
                md.put("skadi-uncompressed-bytes", String.valueOf(chunk.uncompressedBytes));

                int attempt = 0;
                while (true) {
                    try {
                        if (opt.useMultipartAboveBytes() > 0 && chunk.payload.length >= opt.useMultipartAboveBytes()) {
                            s3.multipartUpload(
                                    chunk.ref,
                                    new ByteArrayInputStream(chunk.payload),
                                    chunk.payload.length,
                                    chunk.contentType,
                                    md
                            );
                            logger.info("Uploaded chunk part {} using multipart ({} bytes)", chunk.partNumber, chunk.payload.length);
                        } else {
                            s3.putBytes(chunk.ref, chunk.payload, chunk.contentType, md);
                            logger.info("Uploaded chunk {} part {} ({} bytes)", chunk.ref,chunk.partNumber, chunk.payload.length);
                        }

                        out.add(new ChunkDescriptor(
                                chunk.partNumber,
                                chunk.ref.bucket(),
                                chunk.ref.key(),
                                chunk.payload.length,
                                chunk.uncompressedBytes,
                                chunk.rowCount
                        ));
                        logger.info("Recorded chunk part {} metadata", chunk.partNumber);
                        break;
                    } catch (Exception e) {
                        attempt++;
                        if (attempt > opt.uploadRetries()) throw e;
                        sleepQuietly(opt.retryBackoffMillis(attempt));
                    }
                }
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
            } finally {
                inflightBytes.release(chunk.payload.length);
            }
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------- Plan / Options / Manifest -------------------

    public record S3WritePlan(String bucket, String prefix, String runId) {
        public S3WritePlan {
            Objects.requireNonNull(bucket);
            Objects.requireNonNull(prefix);
            Objects.requireNonNull(runId);
        }

        public S3Models.ObjectRef manifestRef() {
            return new S3Models.ObjectRef(bucket, clean(prefix) + "/" + runId + "/manifest.json");
        }

        public S3Models.ObjectRef chunkRef(int partNumber, String ext) {
            String key = clean(prefix) + "/" + runId + "/chunks/part-" + String.format("%08d", partNumber) + ext;
            return new S3Models.ObjectRef(bucket, key);
        }

        private static String clean(String p) {
            if (p.endsWith("/")) return p.substring(0, p.length() - 1);
            return p;
        }
    }

    public record S3ResultSetRef(String bucket, String manifestKey, String runId, long rowCount, int chunkCount) {
    }

    public interface RowEncoder {
        void writeRow(ResultSet rs, ByteArrayOutputStream out) throws Exception;

        String contentType(boolean compressed);

        String fileExtension(boolean compressed);
    }

    public static final class NdjsonRowEncoder implements RowEncoder {
        @Override
        public void writeRow(ResultSet rs, ByteArrayOutputStream out) throws Exception {
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();

            StringBuilder sb = new StringBuilder(256);
            sb.append('{');
            for (int i = 1; i <= n; i++) {
                if (i > 1) sb.append(',');
                String name = md.getColumnLabel(i);
                Object val = rs.getObject(i);

                sb.append('"').append(escape(name)).append('"').append(':');
                sb.append(toJson(val));
            }
            sb.append('}').append('\n');

            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String contentType(boolean compressed) {
            return "application/x-ndjson";
        }

        @Override
        public String fileExtension(boolean compressed) {
            return compressed ? ".ndjson.gz" : ".ndjson";
        }

        private static String toJson(Object v) {
            if (v == null) return "null";
            if (v instanceof Number || v instanceof Boolean) return v.toString();
            return "\"" + escape(String.valueOf(v)) + "\"";
        }

        private static String escape(String s) {
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    public interface ManifestSerializer {
        byte[] serialize(Manifest m);
    }

    public record StreamOptions(
            int targetChunkBytes,
            int maxInFlightChunks,
            int maxInFlightBytes,
            int uploadThreads,
            int uploadRetries,
            int jdbcFetchSize,
            boolean compress,
            int useMultipartAboveBytes,
            RowEncoder rowEncoder,
            ManifestSerializer manifestSerializer
    ) {
        public static StreamOptions defaults() {
            return new StreamOptions(
                    128 * 1024 * 1024, // chunk target
                    6,                 // max queued chunks
                    512 * 1024 * 1024, // max in-flight bytes (memory cap)
                    4,                 // uploader threads
                    3,                 // retries
                    10_000,            // JDBC fetch size
                    true,              // gzip
                    0,                 // multipart threshold; set e.g. 256*1024*1024 if desired
                    new NdjsonRowEncoder(),
                    m -> minimalJson(m).getBytes(StandardCharsets.UTF_8)
            );
        }

        long retryBackoffMillis(int attempt) {
            return 250L * (1L << Math.min(attempt - 1, 4));
        }

        private static String minimalJson(Manifest m) {
            StringBuilder sb = new StringBuilder(512);
            sb.append("{\"runId\":\"").append(m.runId()).append("\",")
                    .append("\"createdAt\":\"").append(m.createdAt()).append("\",")
                    .append("\"totalRows\":").append(m.totalRows()).append(",")
                    .append("\"totalUncompressedBytes\":").append(m.totalUncompressedBytes()).append(",")
                    .append("\"chunks\":[");
            for (int i = 0; i < m.chunks().size(); i++) {
                ChunkDescriptor c = m.chunks().get(i);
                if (i > 0) sb.append(',');
                sb.append("{\"part\":").append(c.partNumber())
                        .append(",\"bucket\":\"").append(c.bucket()).append("\"")
                        .append(",\"key\":\"").append(c.key()).append("\"")
                        .append(",\"bytes\":").append(c.objectBytes())
                        .append(",\"uncompressedBytes\":").append(c.uncompressedBytes())
                        .append(",\"rows\":").append(c.rowCount())
                        .append("}");
            }
            sb.append("]}");
            return sb.toString();
        }
    }

    public record ChunkDescriptor(
            int partNumber,
            String bucket,
            String key,
            long objectBytes,
            long uncompressedBytes,
            long rowCount
    ) {
    }

    public record Manifest(
            String runId,
            String createdAt,
            String sql,
            long totalRows,
            long totalUncompressedBytes,
            List<ChunkDescriptor> chunks
    ) {
    }

    // ------------------- Chunk building -------------------

    private static final class ChunkBuilder {
        private final ByteArrayOutputStream baos;
        private long rowCount = 0;

        ChunkBuilder(int initialSizeHint) {
            // We allocate modestly; ByteArrayOutputStream will grow as needed.
            this.baos = new ByteArrayOutputStream(Math.min(initialSizeHint, 4 * 1024 * 1024));
        }

        ByteArrayOutputStream out() {
            rowCount++;
            return baos;
        }

        int size() {
            return baos.size();
        }

        SealedChunk seal(S3Models.ObjectRef ref, String contentType, boolean compress, int partNumber) throws Exception {
            byte[] raw = baos.toByteArray();
            long uncompressed = raw.length;

            byte[] payload;
            if (compress) {
                ByteArrayOutputStream gz = new ByteArrayOutputStream(Math.max(1024, raw.length / 3));
                try (GZIPOutputStream gzos = new GZIPOutputStream(gz)) {
                    gzos.write(raw);
                }
                payload = gz.toByteArray();
            } else {
                payload = raw;
            }

            return new SealedChunk(partNumber, ref, payload, contentType, uncompressed, rowCount);
        }

        void reset() {
            baos.reset();
            rowCount = 0;
        }
    }

    private record SealedChunk(
            int partNumber,
            S3Models.ObjectRef ref,
            byte[] payload,
            String contentType,
            long uncompressedBytes,
            long rowCount
    ) {
        private static final SealedChunk POISON =
                new SealedChunk(-1, new S3Models.ObjectRef("", "__POISON__"), new byte[0], "", 0, 0);
    }
}

