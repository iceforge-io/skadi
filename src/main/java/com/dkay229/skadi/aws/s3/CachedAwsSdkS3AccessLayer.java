package com.dkay229.skadi.aws.s3;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CachedAwsSdkS3AccessLayer implements S3AccessLayer {
    private static final Logger logger = LoggerFactory.getLogger(CachedAwsSdkS3AccessLayer.class);
    private final ConcurrentHashMap<Path, CacheMetadata> metadataMap = new ConcurrentHashMap<>();
    private final AwsSdkS3AccessLayer delegate;


    private long currentCacheSize;

    @Value("${skadi.local.cacheMaxSize}")
    private String cacheMaxSize="9";
    private long maxCapacityBytes;

    @Value("${skadi.local.cacheRootDir}")
    private String cacheRootDir;
    private Path cacheDir;

    @Autowired
    public CachedAwsSdkS3AccessLayer(AwsSdkS3AccessLayer delegate) {
        this.delegate = delegate;
    }
    /** used only by non-spring tests **/
    public CachedAwsSdkS3AccessLayer(AwsSdkS3AccessLayer delegate,String cacheMaxSize,String cacheRootDir) {
        this.delegate = delegate;
        this.cacheMaxSize=cacheMaxSize;
        this.cacheRootDir=cacheRootDir;
        init();
    }

    public ConcurrentHashMap<Path, CacheMetadata> getMetadataMap() {
        return metadataMap;
    }

    @PostConstruct
    public void init() {
        this.maxCapacityBytes = DataSizeExpressionEvaluator.evaluate(cacheMaxSize);
        logger.info("Initialized cache with max capacity: {} bytes from property value {}", maxCapacityBytes, cacheMaxSize);
        this.cacheDir= Path.of(this.cacheRootDir);
        logger.info("Cache directory set to: {}", cacheDir);
        try {
            Files.createDirectories(cacheDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create cache directory: " + cacheDir, e);
        }
    }

    @Override
    public byte[] getBytes(S3Models.ObjectRef ref) {
        Path cacheFile = cacheDir.resolve(ref.bucket() + "_" + ref.key().replace("/", "_"));

        if (Files.exists(cacheFile)) {
            try {
                logger.info("Cache hit for s3://{}/{}", ref.bucket(), ref.key());
                metadataMap.get(cacheFile).addAccessTime(); // Update metadata
                return Files.readAllBytes(cacheFile);
            } catch (IOException e) {
                logger.warn("Failed to read cache file: {}", cacheFile, e);
            }
        }

        byte[] data = delegate.getBytes(ref);
        cacheData(cacheFile, data);
        return data;
    }

    private void cacheData(Path cacheFile, byte[] data) {
        try {
            evictIfNeeded(data.length);
            Files.write(cacheFile, data);
            currentCacheSize += data.length;
            metadataMap.put(cacheFile, new CacheMetadata()); // Add metadata
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
                    Path fileToEvict = oldestFile.get();
                    long fileSize = Files.size(fileToEvict);
                    Files.delete(fileToEvict);
                    currentCacheSize -= fileSize;
                    logger.info("Evicted cache file: {} (size: {} bytes)", fileToEvict, fileSize);
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
                    .peek(file -> logger.debug("Eviction candidate: {}", file))
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
            logger.info("Deleted cache file {} for s3://{}/{}",cacheFile,ref.bucket(), ref.key());
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