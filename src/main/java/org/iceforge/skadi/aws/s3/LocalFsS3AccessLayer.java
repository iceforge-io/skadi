package org.iceforge.skadi.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Local filesystem implementation of {@link S3AccessLayer}.
 *
 * <p>This is intended for dev / integration-test mode so Skadi can run without any
 * S3 dependency. Paths are mapped as:
 * <pre>
 *   {localBaseDir}/{bucket}/{key}
 * </pre>
 */
@Service
@Primary
@ConditionalOnProperty(prefix = "skadi.query-cache", name = "store", havingValue = "local")
public class LocalFsS3AccessLayer implements S3AccessLayer {
    private static final Logger log = LoggerFactory.getLogger(LocalFsS3AccessLayer.class);

    private final QueryCachePropertiesView props;

    public LocalFsS3AccessLayer(org.iceforge.skadi.query.QueryCacheProperties cacheProps) {
        this.props = new QueryCachePropertiesView(cacheProps.getLocalBaseDir());
        try {
            Files.createDirectories(Path.of(this.props.localBaseDir));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create localBaseDir=" + this.props.localBaseDir, e);
        }
        log.info("Using LOCAL object store: baseDir={}", this.props.localBaseDir);
    }

    private Path pathFor(S3Models.ObjectRef ref) {
        // Prevent path traversal by normalizing and verifying base prefix.
        Path base = Path.of(props.localBaseDir).toAbsolutePath().normalize();
        Path p = base.resolve(ref.bucket()).resolve(ref.key()).normalize();
        if (!p.startsWith(base)) {
            throw new IllegalArgumentException("Illegal key (path traversal): bucket=" + ref.bucket() + " key=" + ref.key());
        }
        return p;
    }

    @Override
    public String putBytes(S3Models.ObjectRef ref, byte[] bytes, String contentType, Map<String, String> userMetadata) {
        try {
            Path dst = pathFor(ref);
            Files.createDirectories(dst.getParent());
            Path tmp = Files.createTempFile(dst.getParent(), "skadi-", ".tmp");
            Files.write(tmp, bytes);
            Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return "local-etag-" + bytes.length;
        } catch (IOException e) {
            throw new RuntimeException("Local putBytes failed for " + ref, e);
        }
    }

    @Override
    public String putStream(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        try {
            Path dst = pathFor(ref);
            Files.createDirectories(dst.getParent());
            Path tmp = Files.createTempFile(dst.getParent(), "skadi-", ".tmp");
            try (InputStream src = in) {
                Files.copy(src, tmp, StandardCopyOption.REPLACE_EXISTING);
            }
            Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return "local-etag-" + contentLength;
        } catch (IOException e) {
            throw new RuntimeException("Local putStream failed for " + ref, e);
        }
    }

    @Override
    public byte[] getBytes(S3Models.ObjectRef ref) {
        try {
            return Files.readAllBytes(pathFor(ref));
        } catch (IOException e) {
            throw new RuntimeException("Local getBytes failed for " + ref, e);
        }
    }

    @Override
    public InputStream getStream(S3Models.ObjectRef ref) {
        try {
            return Files.newInputStream(pathFor(ref));
        } catch (IOException e) {
            throw new RuntimeException("Local getStream failed for " + ref, e);
        }
    }

    @Override
    public Optional<S3Models.ObjectMetadata> head(S3Models.ObjectRef ref) {
        try {
            Path p = pathFor(ref);
            if (!Files.exists(p)) return Optional.empty();
            long len = Files.size(p);
            Instant lm = Files.getLastModifiedTime(p).toInstant();
            // Provide eTag and contentType to match the 7-arg constructor
            String eTag = "local-etag-" + len;
            String contentType = null;
            return Optional.of(new S3Models.ObjectMetadata(ref.bucket(), ref.key(), len, eTag, contentType, lm, Map.of()));
        } catch (IOException e) {
            throw new RuntimeException("Local head failed for " + ref, e);
        }
    }

    @Override
    public boolean exists(S3Models.ObjectRef ref) {
        return Files.exists(pathFor(ref));
    }

    @Override
    public void delete(S3Models.ObjectRef ref) {
        try {
            Files.deleteIfExists(pathFor(ref));
        } catch (IOException e) {
            throw new RuntimeException("Local delete failed for " + ref, e);
        }
    }

    @Override
    public String copy(S3Models.ObjectRef from, S3Models.ObjectRef to) {
        try {
            Path src = pathFor(from);
            Path dst = pathFor(to);
            Files.createDirectories(dst.getParent());
            Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
            return "local-etag-copy";
        } catch (IOException e) {
            throw new RuntimeException("Local copy failed from=" + from + " to=" + to, e);
        }
    }

    @Override
    public List<S3Models.ListItem> list(String bucket, String prefix, int maxKeys) {
        Path base = Path.of(props.localBaseDir).toAbsolutePath().normalize();
        Path root = base.resolve(bucket).resolve(prefix).normalize();
        if (!root.startsWith(base)) {
            throw new IllegalArgumentException("Illegal list prefix: " + prefix);
        }
        if (!Files.exists(root)) return List.of();
        try (var stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .limit(Math.max(0, maxKeys))
                    .map(p -> {
                        String key = base.resolve(bucket).relativize(p).toString().replace('\\', '/');
                        try {
                            long sz = Files.size(p);
                            Instant lm = Files.getLastModifiedTime(p).toInstant();
                            // ListItem signature expects (key, size, bucket, lastModified)
                            return new S3Models.ListItem(key, sz, bucket, lm);
                        } catch (IOException e) {
                            return new S3Models.ListItem(key, -1, bucket, Instant.EPOCH);
                        }
                    })
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException("Local list failed bucket=" + bucket + " prefix=" + prefix, e);
        }
    }

    @Override
    public URL presignGet(S3Models.ObjectRef ref, Duration ttl) {
        throw new UnsupportedOperationException("presignGet not supported for local store");
    }

    @Override
    public URL presignPut(S3Models.ObjectRef ref, Duration ttl, String contentType) {
        throw new UnsupportedOperationException("presignPut not supported for local store");
    }

    @Override
    public String multipartUpload(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        // Not needed for local; treat as a normal stream upload.
        return putStream(ref, in, contentLength, contentType, userMetadata);
    }

    private static final class QueryCachePropertiesView {
        private final String localBaseDir;

        private QueryCachePropertiesView(String localBaseDir) {
            this.localBaseDir = localBaseDir;
        }
    }
}