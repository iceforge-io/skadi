package com.dkay229.skadi.aws.s3;

import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface S3AccessLayer {

    // Upload
    String putBytes(S3Models.ObjectRef ref, byte[] bytes, String contentType, Map<String, String> userMetadata);
    String putStream(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata);

    // Download
    byte[] getBytes(S3Models.ObjectRef ref);
    InputStream getStream(S3Models.ObjectRef ref); // caller closes

    // Metadata / existence
    Optional<S3Models.ObjectMetadata> head(S3Models.ObjectRef ref);
    boolean exists(S3Models.ObjectRef ref);

    // Delete
    void delete(S3Models.ObjectRef ref);

    // Copy / move helpers
    String copy(S3Models.ObjectRef from, S3Models.ObjectRef to);
    default String move(S3Models.ObjectRef from, S3Models.ObjectRef to) {
        String etag = copy(from, to);
        delete(from);
        return etag;
    }

    // List
    List<S3Models.ListItem> list(String bucket, String prefix, int maxKeys);

    // Presigned URLs
    URL presignGet(S3Models.ObjectRef ref, Duration ttl);
    URL presignPut(S3Models.ObjectRef ref, Duration ttl, String contentType);

    // Optional: multipart upload for large streams/files
    String multipartUpload(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata);
}
