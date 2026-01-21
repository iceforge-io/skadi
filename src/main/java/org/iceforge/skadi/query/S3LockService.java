package org.iceforge.skadi.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

@Service
@ConditionalOnProperty(prefix = "skadi.query-cache", name = "store", havingValue = "s3", matchIfMissing = true)
public class S3LockService implements LockService {

    private static final Logger log = LoggerFactory.getLogger(S3LockService.class);

    private final S3Client s3;

    public S3LockService(S3Client s3) {
        this.s3 = Objects.requireNonNull(s3);
    }

    /**
     * Best-effort cross-instance lock using a small S3 object.
     * <p>
     * Semantics:
     * - If lock object exists → return false
     * - Else write lock object and return true
     */
    @Override
    public boolean tryAcquire(String bucket, String lockKey, String ownerId, long ttlSeconds) {
        Objects.requireNonNull(bucket);
        Objects.requireNonNull(lockKey);

        // 1) Check existence
        try {
            s3.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(lockKey)
                    .build());
            return false; // already locked
        } catch (NoSuchKeyException e) {
            // expected → continue
        } catch (S3Exception e) {
            if (e.statusCode() != 404) {
                throw e;
            }
        }

        // 2) Write lock object
        String body = "{\"owner\":\"" + ownerId + "\",\"startedAt\":\"" +
                Instant.now() + "\",\"ttlSeconds\":" + ttlSeconds + "}";

        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucket)
                .key(lockKey)
                .contentType("application/json")
                .metadata(Map.of(
                        "skadi-owner", ownerId == null ? "" : ownerId,
                        "skadi-ttlSeconds", String.valueOf(ttlSeconds)
                ))
                .build();

        try {
            s3.putObject(put, RequestBody.fromBytes(body.getBytes(StandardCharsets.UTF_8)));
            return true;
        } catch (S3Exception e) {
            log.warn("Failed to create lock object s3://{}/{}: {}",
                    bucket, lockKey, e.getMessage());
            return false;
        }
    }

    @Override
    public void release(String bucket, String lockKey) {
        try {
            s3.deleteObject(b -> b.bucket(bucket).key(lockKey));
        } catch (Exception e) {
            log.debug("Ignoring lock release failure s3://{}/{}", bucket, lockKey, e.toString());
        }
    }
}
