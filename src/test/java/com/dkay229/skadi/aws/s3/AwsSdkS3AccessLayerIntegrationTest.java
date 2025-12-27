package com.dkay229.skadi.aws.s3;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class AwsSdkS3AccessLayerIntegrationTest {

    @Value("${aws.s3.perf-test-bucket.name}")
    private String testBucket;

    private final S3Client s3Client = S3Client.create();

    @Test
    void testPerformanceAndVolume() {
        String keyPrefix = "test-performance/";
        int fileCount = 100; // Number of files to upload
        int fileSizeKB = 10; // Size of each file in KB

        // Upload files
        for (int i = 0; i < fileCount; i++) {
            String key = keyPrefix + UUID.randomUUID();
            byte[] data = new byte[fileSizeKB * 1024];
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(testBucket)
                    .key(key)
                    .build();

            long startTime = System.nanoTime();
            s3Client.putObject(request, RequestBody.fromBytes(data));
            long duration = Duration.ofNanos(System.nanoTime() - startTime).toMillis();

            System.out.println("Uploaded file " + key + " in " + duration + " ms");
        }

        // List files
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(testBucket)
                .prefix(keyPrefix)
                .build();
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        assertEquals(fileCount, listResponse.contents().size(), "File count mismatch");

        // Clean up
        for (S3Object object : listResponse.contents()) {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(testBucket)
                    .key(object.key())
                    .build());
        }
    }
    @Test
    void testMultipartUploadPerformance() {
        for (int numParts = 1; numParts <= 15; numParts += 5) {
            String key = "test-multipart/" + UUID.randomUUID();
            long fileSize = 100L * 1024 * 1024; // 100 MB
            long partSize = fileSize / numParts; // Calculate part size based on the number of parts
            byte[] data = new byte[(int) partSize];

            // Initialize multipart upload
            CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                    .bucket(testBucket)
                    .key(key)
                    .build();
            CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(createRequest);
            String uploadId = createResponse.uploadId();

            try {
                // Upload parts in parallel
                long startTime = System.nanoTime();
                List<CompletableFuture<CompletedPart>> uploadFutures = new ArrayList<>();
                for (int partNumber = 1; partNumber <= numParts; partNumber++) {
                    final int currentPartNumber = partNumber;
                    uploadFutures.add(CompletableFuture.supplyAsync(() -> {
                        UploadPartRequest uploadRequest = UploadPartRequest.builder()
                                .bucket(testBucket)
                                .key(key)
                                .uploadId(uploadId)
                                .partNumber(currentPartNumber)
                                .contentLength((long) data.length)
                                .build();
                        try {
                            UploadPartResponse uploadResponse = s3Client.uploadPart(uploadRequest, RequestBody.fromBytes(data));
                            return CompletedPart.builder()
                                    .partNumber(currentPartNumber)
                                    .eTag(uploadResponse.eTag())
                                    .build();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to upload part " + currentPartNumber, e);
                        }
                    }));
                }

                // Wait for all parts to complete
                List<CompletedPart> completedParts = uploadFutures.stream()
                        .map(CompletableFuture::join)
                        .toList();

                // Complete multipart upload
                CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                        .bucket(testBucket)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                        .build();
                s3Client.completeMultipartUpload(completeRequest);

                long durationNanos = System.nanoTime() - startTime;
                double durationSeconds = durationNanos / 1_000_000_000.0;
                double speedMbps = (fileSize * 8) / durationSeconds / 1_000_000;

                System.out.printf("Uploaded 100 MB file in %d parts at %.2f Mbps%n", numParts, speedMbps);

            } catch (Exception e) {
                // Abort multipart upload in case of failure
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(testBucket)
                        .key(key)
                        .uploadId(uploadId)
                        .build());
                fail("Multipart upload failed for " + numParts + " parts: " + e.getMessage());
            } finally {
                // Delete the file after the test
                s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(testBucket)
                        .key(key)
                        .build());
                System.out.println("Deleted file: " + key);
            }
        }
    }
}