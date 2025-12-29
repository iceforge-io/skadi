package com.dkay229.skadi.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class CachedAwsSdkS3AccessLayer implements S3AccessLayer {
    private static final Logger logger = LoggerFactory.getLogger(CachedAwsSdkS3AccessLayer.class);
    private final AwsSdkS3AccessLayer delegate;
    private final Path cacheDir;
    private final long maxCapacityBytes;
    private long currentCacheSize;

    public CachedAwsSdkS3AccessLayer(AwsSdkS3AccessLayer delegate, Path cacheDir, long maxCapacityBytes) {
        this.delegate = delegate;
        this.cacheDir = cacheDir;
        this.maxCapacityBytes = maxCapacityBytes;
        this.currentCacheSize = calculateCurrentCacheSize();

        try {
            Files.createDirectories(cacheDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create cache directory: " + cacheDir, e);
        }
    }

    @Override
    public byte[] getBytes(S3Models.ObjectRef ref) {
        Path cacheFile = cacheDir.resolve(ref.bucket() + "_" + ref.key().replace("/", "_"));

        // Check cache
        if (Files.exists(cacheFile)) {
            try {
                logger.info("Cache hit for s3://{}/{}", ref.bucket(), ref.key());
                return Files.readAllBytes(cacheFile);
            } catch (IOException e) {
                logger.warn("Failed to read cache file: {}", cacheFile, e);
            }
        }

        // Fetch from S3 and cache
        byte[] data = delegate.getBytes(ref);
        cacheData(cacheFile, data);
        return data;
    }

    private void cacheData(Path cacheFile, byte[] data) {
        try {
            evictIfNeeded(data.length);
            Files.write(cacheFile, data);
            currentCacheSize += data.length;
            logger.info("Cached s3://{} to {}", cacheFile.getFileName(), cacheFile);
        } catch (IOException e) {
            logger.warn("Failed to write cache file: {}", cacheFile, e);
        }
    }

    private void evictIfNeeded(long newDataSize) {
        while (currentCacheSize + newDataSize > maxCapacityBytes) {
            Optional<Path> oldestFile = getOldestCacheFile();
            if (oldestFile.isPresent()) {
                try {
                    long fileSize = Files.size(oldestFile.get());
                    Files.delete(oldestFile.get());
                    currentCacheSize -= fileSize;
                    logger.info("Evicted cache file: {}", oldestFile.get());
                } catch (IOException e) {
                    logger.warn("Failed to evict cache file: {}", oldestFile.get(), e);
                }
            } else {
                logger.warn("No files to evict, but cache is over capacity!");
                break;
            }
        }
    }

    private Optional<Path> getOldestCacheFile() {
        try {
            return Files.list(cacheDir)
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparingLong(this::getFileCreationTime))
                    .findFirst();
        } catch (IOException e) {
            logger.warn("Failed to list cache files for eviction", e);
            return Optional.empty();
        }
    }

    private long getFileCreationTime(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            logger.warn("Failed to get creation time for file: {}", file, e);
            return Long.MAX_VALUE;
        }
    }

    private long calculateCurrentCacheSize() {
        try {
            return Files.list(cacheDir)
                    .filter(Files::isRegularFile)
                    .mapToLong(this::getFileSize)
                    .sum();
        } catch (IOException e) {
            logger.warn("Failed to calculate current cache size", e);
            return 0;
        }
    }

    private long getFileSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            logger.warn("Failed to get file size for: {}", file, e);
            return 0;
        }
    }

    @Override
    public void delete(S3Models.ObjectRef ref) {
        delegate.delete(ref);
        Path cacheFile = cacheDir.resolve(ref.bucket() + "_" + ref.key().replace("/", "_"));
        try {
            long fileSize = Files.size(cacheFile);
            Files.deleteIfExists(cacheFile);
            currentCacheSize -= fileSize;
            logger.info("Deleted cache for s3://{}/{}", ref.bucket(), ref.key());
        } catch (IOException e) {
            logger.warn("Failed to delete cache file: {}", cacheFile, e);
        }
    }

    // Delegate other methods to the wrapped AwsSdkS3AccessLayer

    @Override
    public String putBytes(S3Models.ObjectRef ref, byte[] bytes, String contentType, Map<String, String> userMetadata) {
        return delegate.putBytes(ref, bytes, contentType, userMetadata);
    }

    @Override
    public String putStream(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        return delegate.putStream(ref, in, contentLength, contentType, userMetadata);
    }

    @Override
    public InputStream getStream(S3Models.ObjectRef ref) {
        return delegate.getStream(ref);
    }

    @Override
    public Optional<S3Models.ObjectMetadata> head(S3Models.ObjectRef ref) {
        return delegate.head(ref);
    }

    @Override
    public boolean exists(S3Models.ObjectRef ref) {
        return delegate.exists(ref);
    }

    @Override
    public String copy(S3Models.ObjectRef from, S3Models.ObjectRef to) {
        return delegate.copy(from, to);
    }

    @Override
    public List<S3Models.ListItem> list(String bucket, String prefix, int maxKeys) {
        return delegate.list(bucket, prefix, maxKeys);
    }

    @Override
    public URL presignGet(S3Models.ObjectRef ref, Duration ttl) {
        return delegate.presignGet(ref, ttl);
    }

    @Override
    public URL presignPut(S3Models.ObjectRef ref, Duration ttl, String contentType) {
        return delegate.presignPut(ref, ttl, contentType);
    }

    @Override
    public String multipartUpload(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        return delegate.multipartUpload(ref, in, contentLength, contentType, userMetadata);
    }
}