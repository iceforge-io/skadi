package com.dkay229.skadi.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
    public ResultSetToS3ChunkWriter(S3AccessLayer s3) {
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

                // Flush trailing partial
                if (builder.size() > 0) {
                    part++;
                    SealedChunk sealed = builder.seal(
                            plan.chunkRef(part, encoder.fileExtension(opt.compress())),
                            encoder.contentType(opt.compress()),
                            opt.compress(),
                            part
                    );

                    inflightBytes.acquire(sealed.payload.length);
                    queue.put(sealed);

                    totalUncompressedBytes += sealed.uncompressedBytes;
                }
            }
        } catch (Throwable t) {
            firstError.compareAndSet(null, t);
            throw t;
        } finally {
            // Poison pills to stop workers
            for (int i = 0; i < opt.uploadThreads(); i++) {
                queue.offer(SealedChunk.poison());
            }

            uploadPool.shutdown();

            for (Future<?> f : workers) {
                try {
                    f.get();
                } catch (Exception e) {
                    firstError.compareAndSet(null, e);
                }
            }
        }

        Throwable err = firstError.get();
        if (err != null) {
            throw (err instanceof Exception) ? (Exception) err : new RuntimeException(err);
        }

        // Build & upload manifest last
        Manifest manifest = new Manifest(
                plan.runId(),
                plan.bucket(),
                plan.prefix(),
                opt.compress(),
                totalRows,
                totalUncompressedBytes,
                chunks
        );

        byte[] manifestBytes = opt.manifestWriter().write(manifest);
        S3Models.ObjectRef manifestRef = plan.manifestRef();
        s3.putBytes(manifestRef, manifestBytes, "application/json", Map.of());

        logger.info("Completed ResultSet->S3 write: rows={}, chunks={}, manifest=s3://{}/{}",
                totalRows, chunks.size(), manifestRef.bucket(), manifestRef.key()
        );

        return new S3ResultSetRef(
                plan.bucket(),
                plan.prefix(),
                plan.runId(),
                manifestRef.key(),
                totalRows,
                chunks.size()
        );
    }

    private void uploaderLoop(
            S3WritePlan plan,
            StreamOptions opt,
            BlockingQueue<SealedChunk> queue,
            Semaphore inflightBytes,
            List<ChunkDescriptor> chunks,
            AtomicReference<Throwable> firstError
    ) {
        while (true) {
            SealedChunk chunk;
            try {
                chunk = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (chunk.poison) return;
            if (firstError.get() != null) return;

            try {
                Map<String, String> userMeta = new HashMap<>();
                userMeta.put("skadi-runId", plan.runId());
                userMeta.put("skadi-part", String.valueOf(chunk.part));

                String etag = s3.putBytes(chunk.ref, chunk.payload, chunk.contentType, userMeta);

                chunks.add(new ChunkDescriptor(
                        chunk.part,
                        chunk.ref.key(),
                        chunk.payload.length,
                        chunk.uncompressedBytes,
                        etag
                ));
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
                return;
            } finally {
                inflightBytes.release(chunk.payload.length);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Types below are unchanged from your tarball
    // --------------------------------------------------------------------------------------------

    public record S3WritePlan(String bucket, String prefix, String runId) {
        public S3Models.ObjectRef manifestRef() {
            return new S3Models.ObjectRef(bucket, prefix + "/" + runId + "/manifest.json");
        }

        public S3Models.ObjectRef chunkRef(int part, String ext) {
            return new S3Models.ObjectRef(bucket, prefix + "/" + runId + "/part-" + String.format("%06d", part) + ext);
        }
    }

    public record S3ResultSetRef(
            String bucket,
            String prefix,
            String runId,
            String manifestKey,
            long rowCount,
            int chunkCount
    ) {}


    public record ChunkDescriptor(int part, String key, long bytes, long uncompressedBytes, String etag) {}

    public record Manifest(
            String runId,
            String bucket,
            String prefix,
            boolean compressed,
            long totalRows,
            long totalUncompressedBytes,
            List<ChunkDescriptor> chunks
    ) {}

    public interface ManifestWriter {
        byte[] write(Manifest manifest);
    }

    public interface RowEncoder {
        void writeRow(ResultSet rs, ByteArrayOutputStream out) throws Exception;
        String contentType(boolean compressed);
        String fileExtension(boolean compressed);
    }

    public record StreamOptions(
            int jdbcFetchSize,
            int uploadThreads,
            int maxInFlightChunks,
            int maxInFlightBytes,
            int targetChunkBytes,
            boolean compress,
            RowEncoder rowEncoder,
            ManifestWriter manifestWriter
    ) {
        public static StreamOptions defaults() {
            return new StreamOptions(
                    1000,
                    4,
                    16,
                    64 * 1024 * 1024,
                    8 * 1024 * 1024,
                    true,
                    new JsonLinesRowEncoder(),
                    new DefaultManifestWriter()
            );
        }
    }

    private static final class SealedChunk {
        final S3Models.ObjectRef ref;
        final byte[] payload;
        final String contentType;
        final long uncompressedBytes;
        final int part;
        final boolean poison;

        private SealedChunk(S3Models.ObjectRef ref, byte[] payload, String contentType, long uncompressedBytes, int part, boolean poison) {
            this.ref = ref;
            this.payload = payload;
            this.contentType = contentType;
            this.uncompressedBytes = uncompressedBytes;
            this.part = part;
            this.poison = poison;
        }

        static SealedChunk poison() {
            return new SealedChunk(new S3Models.ObjectRef("",""), new byte[0], "application/octet-stream", 0, -1, true);
        }
    }

    private static final class ChunkBuilder {
        private final int targetBytes;
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        ChunkBuilder(int targetBytes) {
            this.targetBytes = targetBytes;
        }

        ByteArrayOutputStream out() {
            return out;
        }

        int size() {
            return out.size();
        }

        SealedChunk seal(S3Models.ObjectRef ref, String contentType, boolean compress, int part) throws Exception {
            byte[] raw = out.toByteArray();
            long uncompressed = raw.length;

            byte[] payload = raw;
            String ct = contentType;

            if (compress) {
                payload = gzip(raw);
                ct = contentType;
            }

            return new SealedChunk(ref, payload, ct, uncompressed, part, false);
        }

        void reset() {
            out.reset();
        }

        private static byte[] gzip(byte[] raw) throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
                gz.write(raw);
            }
            return bos.toByteArray();
        }
    }

    public static final class DefaultManifestWriter implements ManifestWriter {
        @Override
        public byte[] write(Manifest manifest) {
            // Simple JSON (minimal dependencies)
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"runId\":\"").append(escape(manifest.runId())).append("\",");
            sb.append("\"bucket\":\"").append(escape(manifest.bucket())).append("\",");
            sb.append("\"prefix\":\"").append(escape(manifest.prefix())).append("\",");
            sb.append("\"compressed\":").append(manifest.compressed()).append(",");
            sb.append("\"totalRows\":").append(manifest.totalRows()).append(",");
            sb.append("\"totalUncompressedBytes\":").append(manifest.totalUncompressedBytes()).append(",");
            sb.append("\"chunks\":[");
            for (int i = 0; i < manifest.chunks().size(); i++) {
                ChunkDescriptor c = manifest.chunks().get(i);
                if (i > 0) sb.append(",");
                sb.append("{");
                sb.append("\"part\":").append(c.part()).append(",");
                sb.append("\"key\":\"").append(escape(c.key())).append("\",");
                sb.append("\"bytes\":").append(c.bytes()).append(",");
                sb.append("\"uncompressedBytes\":").append(c.uncompressedBytes()).append(",");
                sb.append("\"etag\":\"").append(escape(c.etag())).append("\"");
                sb.append("}");
            }
            sb.append("]");
            sb.append("}");
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        private static String escape(String s) {
            if (s == null) return "";
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    public static final class JsonLinesRowEncoder implements RowEncoder {
        @Override
        public void writeRow(ResultSet rs, ByteArrayOutputStream out) throws Exception {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();

            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 1; i <= cols; i++) {
                if (i > 1) sb.append(",");
                String name = md.getColumnLabel(i);
                Object val = rs.getObject(i);

                sb.append("\"").append(name.replace("\"", "\\\"")).append("\":");
                if (val == null) {
                    sb.append("null");
                } else if (val instanceof Number || val instanceof Boolean) {
                    sb.append(val);
                } else {
                    sb.append("\"").append(val.toString().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                }
            }
            sb.append("}\n");
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String contentType(boolean compressed) {
            return compressed ? "application/x-ndjson+gzip" : "application/x-ndjson";
        }

        @Override
        public String fileExtension(boolean compressed) {
            return compressed ? ".ndjson.gz" : ".ndjson";
        }
    }
}
