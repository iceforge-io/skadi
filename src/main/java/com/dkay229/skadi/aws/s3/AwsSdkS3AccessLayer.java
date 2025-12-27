package com.dkay229.skadi.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.*;

@Service
public class AwsSdkS3AccessLayer implements S3AccessLayer {
    private static final Logger logger = LoggerFactory.getLogger(AwsSdkS3AccessLayer.class);
    private final S3Client s3;
    private final S3Presigner presigner;

    public AwsSdkS3AccessLayer(S3Client s3, S3Presigner presigner) {
        this.s3 = s3;
        this.presigner = presigner;
    }

    @Override
    public String putBytes(S3Models.ObjectRef ref, byte[] bytes, String contentType, Map<String, String> userMetadata) {
        try {
            PutObjectRequest.Builder req = PutObjectRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key());

            if (contentType != null && !contentType.isBlank()) req = req.contentType(contentType);
            if (userMetadata != null && !userMetadata.isEmpty()) req = req.metadata(userMetadata);

            PutObjectResponse resp = s3.putObject(req.build(), RequestBody.fromBytes(bytes));
            return resp.eTag();
        } catch (S3Exception e) {
            logger.error("S3 putBytes failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 putBytes failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public String putStream(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        try {
            PutObjectRequest.Builder req = PutObjectRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key());
            if (contentType != null && !contentType.isBlank()) req = req.contentType(contentType);
            if (userMetadata != null && !userMetadata.isEmpty()) req = req.metadata(userMetadata);

            PutObjectResponse resp = s3.putObject(req.build(), RequestBody.fromInputStream(in, contentLength));
            return resp.eTag();
        } catch (S3Exception e) {
            logger.error("S3 putStream failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 putStream failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public byte[] getBytes(S3Models.ObjectRef ref) {
        try (ResponseInputStream<GetObjectResponse> ris = s3.getObject(
                GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build()
        )) {
            return readAllBytes(ris);
        } catch (NoSuchKeyException e) {
            logger.error("S3 object not found for getBytes s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 object not found: s3://" + ref.bucket() + "/" + ref.key(), e);
        } catch (S3Exception | IOException e) {
            throw new S3AccessException("S3 getBytes failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public InputStream getStream(S3Models.ObjectRef ref) {
        try {
            // Caller must close
            return s3.getObject(GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build());
        } catch (S3Exception e) {
            logger.error("S3 getStream failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 getStream failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public Optional<S3Models.ObjectMetadata> head(S3Models.ObjectRef ref) {
        try {
            HeadObjectResponse r = s3.headObject(HeadObjectRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key())
                    .build());

            return Optional.of(new S3Models.ObjectMetadata(
                    ref.bucket(),
                    ref.key(),
                    r.contentLength(),
                    r.eTag(),
                    r.contentType(),
                    r.lastModified(),
                    r.metadata() == null ? Map.of() : r.metadata()
            ));
        } catch (NoSuchKeyException e) {
            /*
            // Some S3-compatible APIs throw generic 404 as S3Exception; treat 404 as not-found.
            if ( e.statusCode() == 404) return Optional.empty();
            if (e instanceof NoSuchKeyException) return Optional.empty();
             */
            logger.error("S3 object not found for head s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 head failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        } catch (S3Exception e) {
            // Some S3-compatible APIs throw generic 404 as S3Exception; treat 404 as not-found.
            if (e.statusCode() == 404) {
                logger.warn("Some S3-compatible APIs throw generic 404 as S3Exception; treat 404 as not-found for head s3://{}/{}", ref.bucket(), ref.key());
                return Optional.empty();
            }
            logger.error("S3 head failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 head failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public boolean exists(S3Models.ObjectRef ref) {
        return head(ref).isPresent();
    }

    @Override
    public void delete(S3Models.ObjectRef ref) {
        try {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build());
        } catch (S3Exception e) {
            logger.error("S3 delete failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 delete failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public String copy(S3Models.ObjectRef from, S3Models.ObjectRef to) {
        try {
            CopyObjectResponse resp = s3.copyObject(CopyObjectRequest.builder()
                    .copySource(from.bucket() + "/" + from.key())
                    .destinationBucket(to.bucket())
                    .destinationKey(to.key())
                    .build());
            // CopyObjectResponse doesn't always carry ETag uniformly; use head if you need final ETag
            return resp.copyObjectResult() != null ? resp.copyObjectResult().eTag() : "";
        } catch (S3Exception e) {
            logger.error("S3 copy failed from s3://{}/{} to s3://{}/{}", from.bucket(), from.key(), to.bucket(), to.key(), e);
            throw new S3AccessException("S3 copy failed: from s3://" + from.bucket() + "/" + from.key()
                    + " to s3://" + to.bucket() + "/" + to.key(), e);
        }
    }

    @Override
    public List<S3Models.ListItem> list(String bucket, String prefix, int maxKeys) {
        try {
            ListObjectsV2Response r = s3.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix == null ? "" : prefix)
                    .maxKeys(Math.max(1, maxKeys))
                    .build());

            List<S3Models.ListItem> out = new ArrayList<>();
            if (r.contents() != null) {
                for (S3Object o : r.contents()) {
                    out.add(new S3Models.ListItem(o.key(), o.size(), o.eTag(), o.lastModified()));
                }
            }
            return out;
        } catch (S3Exception e) {
            logger.error("S3 list failed for bucket={} prefix={}", bucket, prefix, e);
            throw new S3AccessException("S3 list failed: bucket=" + bucket + " prefix=" + prefix, e);
        }
    }

    @Override
    public URL presignGet(S3Models.ObjectRef ref, Duration ttl) {
        try {
            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key())
                    .build();

            PresignedGetObjectRequest presigned = presigner.presignGetObject(b -> b
                    .signatureDuration(ttl == null ? Duration.ofMinutes(10) : ttl)
                    .getObjectRequest(getReq)
            );
            return presigned.url();
        } catch (Exception e) {
            logger.error("S3 presignGet failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 presignGet failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public URL presignPut(S3Models.ObjectRef ref, Duration ttl, String contentType) {
        try {
            PutObjectRequest.Builder putReq = PutObjectRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key());

            if (contentType != null && !contentType.isBlank()) {
                putReq = putReq.contentType(contentType);
            }
            final PutObjectRequest.Builder putReqFinal = putReq;
            PresignedPutObjectRequest presigned = presigner.presignPutObject(b -> b
                    .signatureDuration(ttl == null ? Duration.ofMinutes(10) : ttl)
                    .putObjectRequest(putReqFinal.build())
            );
            return presigned.url();
        } catch (Exception e) {
            logger.error("S3 presignPut failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            throw new S3AccessException("S3 presignPut failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    /**
     * Multipart upload helper. Useful when contentLength is large and you want to avoid single PUT limits/timeouts.
     * This is a conservative implementation: 8 MiB parts.
     */
    @Override
    public String multipartUpload(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        final long partSize = 8L * 1024 * 1024;

        CreateMultipartUploadResponse init = null;
        try {
            CreateMultipartUploadRequest.Builder initReq = CreateMultipartUploadRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key());

            if (contentType != null && !contentType.isBlank()) initReq = initReq.contentType(contentType);
            if (userMetadata != null && !userMetadata.isEmpty()) initReq = initReq.metadata(userMetadata);

            init = s3.createMultipartUpload(initReq.build());
            String uploadId = init.uploadId();

            List<CompletedPart> completed = new ArrayList<>();
            long bytesRemaining = contentLength;
            int partNumber = 1;

            while (bytesRemaining > 0) {
                long thisPart = Math.min(partSize, bytesRemaining);
                byte[] buf = readExactly(in, thisPart);

                UploadPartResponse up = s3.uploadPart(UploadPartRequest.builder()
                        .bucket(ref.bucket())
                        .key(ref.key())
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .contentLength((long) buf.length)
                        .build(), RequestBody.fromBytes(buf));

                completed.add(CompletedPart.builder().partNumber(partNumber).eTag(up.eTag()).build());

                bytesRemaining -= buf.length;
                partNumber++;
            }

            CompleteMultipartUploadResponse done = s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(ref.bucket())
                    .key(ref.key())
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
                    .build());

            return done.eTag() == null ? "" : done.eTag();
        } catch (Exception e) {
            logger.error("S3 multipartUpload failed for s3://{}/{}", ref.bucket(), ref.key(), e);
            try {
                logger.error("Abort best-effort for multipart upload s3://{}/{}", ref.bucket(), ref.key());
                if (init != null && init.uploadId() != null) {
                    s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                            .bucket(ref.bucket())
                            .key(ref.key())
                            .uploadId(init.uploadId())
                            .build());
                }
            } catch (Exception ignored) { }
            throw new S3AccessException("S3 multipartUpload failed: s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    private static byte[] readAllBytes(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int r;
        while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
        return baos.toByteArray();
    }

    private static byte[] readExactly(InputStream in, long bytes) throws IOException {
        if (bytes > Integer.MAX_VALUE) throw new IllegalArgumentException("Part too large for this helper: " + bytes);

        int target = (int) bytes;
        byte[] out = new byte[target];
        int off = 0;
        while (off < target) {
            int r = in.read(out, off, target - off);
            if (r == -1) break;
            off += r;
        }
        if (off == target) return out;
        return Arrays.copyOf(out, off); // last part might be smaller if upstream lied about length
    }
}
