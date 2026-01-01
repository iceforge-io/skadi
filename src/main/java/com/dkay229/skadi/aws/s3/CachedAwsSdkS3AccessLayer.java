package com.dkay229.skadi.aws.s3;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.*;

@Service
@Primary
public class CachedAwsSdkS3AccessLayer implements S3AccessLayer {
    private static final Logger logger = LoggerFactory.getLogger(CachedAwsSdkS3AccessLayer.class);

    private final ConcurrentHashMap<Path, CacheMetadata> metadataMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    private final AwsSdkS3AccessLayer delegate;
    private final PeerCacheClient peerCacheClient;

    private long currentCacheSize;

    @Value("${skadi.local.cacheMaxSize}")
    private String cacheMaxSize = "9";
    private long maxCapacityBytes;

    @Value("${skadi.local.cacheRootDir}")
    private String cacheRootDir;
    private Path cacheDir;

    // Peer cache config
    @Value("${skadi.peerCache.enabled:false}")
    private boolean peerEnabled;

    @Value("${skadi.peerCache.peers:}")
    private List<String> peerBaseUrls;

    @Value("${skadi.peerCache.maxPeersToTry:2}")
    private int maxPeersToTry;

    @Value("${skadi.peerCache.headTimeoutMs:250}")
    private long headTimeoutMs;

    @Value("${skadi.peerCache.getTimeoutMs:8000}")
    private long getTimeoutMs;

    // HMAC auth config (client-side)
    @Value("${skadi.peerCache.auth.keyId:}")
    private String peerKeyId;

    @Value("#{${skadi.peerCache.auth.sharedSecrets:{}}}")
    private Map<String, String> peerSecrets;

    @Autowired
    public CachedAwsSdkS3AccessLayer(AwsSdkS3AccessLayer delegate, PeerCacheClient peerCacheClient) {
        this.delegate = delegate;
        this.peerCacheClient = peerCacheClient;
    }

    /** used only by non-spring tests **/
    public CachedAwsSdkS3AccessLayer(AwsSdkS3AccessLayer delegate, String cacheMaxSize, String cacheRootDir) {
        this.delegate = delegate;
        this.peerCacheClient = null;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheRootDir = cacheRootDir;
        init();
    }

    public ConcurrentHashMap<Path, CacheMetadata> getMetadataMap() {
        return metadataMap;
    }

    @PostConstruct
    public void init() {
        this.maxCapacityBytes = DataSizeExpressionEvaluator.evaluate(cacheMaxSize);
        logger.info("Initialized cache with max capacity: {} bytes from property value {}", maxCapacityBytes, cacheMaxSize);

        this.cacheDir = Path.of(this.cacheRootDir);
        logger.info("Cache directory set to: {}", cacheDir);

        try {
            Files.createDirectories(cacheDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create cache directory: " + cacheDir, e);
        }

        this.currentCacheSize = calculateCurrentCacheSize();
        logger.info("Current cache size at startup: {} bytes", currentCacheSize);
    }

    /** For PeerCacheController */
    public Optional<Path> localPathIfCached(S3Models.ObjectRef ref) {
        Path p = cachePath(ref);
        return Files.exists(p) ? Optional.of(p) : Optional.empty();
    }

    @Override
    public byte[] getBytes(S3Models.ObjectRef ref) {
        try (InputStream in = getStream(ref)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read bytes for s3://" + ref.bucket() + "/" + ref.key(), e);
        }
    }

    @Override
    public InputStream getStream(S3Models.ObjectRef ref) {
        String lockKey = ref.bucket() + ":" + ref.key();
        Object lock = locks.computeIfAbsent(lockKey, k -> new Object());

        synchronized (lock) {
            Path cacheFile = cachePath(ref);

            // 1) Local
            if (Files.exists(cacheFile)) {
                try {
                    logger.info("Local cache hit for s3://{}/{}", ref.bucket(), ref.key());
                    metadataMap.computeIfAbsent(cacheFile, p -> new CacheMetadata()).addAccessTime();
                    validateMetaOrWarn(ref, cacheFile);
                    return Files.newInputStream(cacheFile);
                } catch (IOException e) {
                    logger.warn("Failed to open cached stream: {}", cacheFile, e);
                }
            }

            // 2) Peer
            if (tryPullFromPeers(ref, cacheFile) && Files.exists(cacheFile)) {
                try {
                    logger.info("Peer cache hit (pulled locally) for s3://{}/{}", ref.bucket(), ref.key());
                    metadataMap.computeIfAbsent(cacheFile, p -> new CacheMetadata()).addAccessTime();
                    return Files.newInputStream(cacheFile);
                } catch (IOException e) {
                    logger.warn("Failed to open cached stream after peer pull: {}", cacheFile, e);
                }
            }

            // 3) S3 fallback (stream to disk, then open)
            try {
                pullFromS3ToLocal(ref, cacheFile);
                metadataMap.computeIfAbsent(cacheFile, p -> new CacheMetadata()).addAccessTime();
                return Files.newInputStream(cacheFile);
            } catch (IOException e) {
                throw new RuntimeException("Failed to fetch and cache s3://" + ref.bucket() + "/" + ref.key(), e);
            }
        }
    }

    private void pullFromS3ToLocal(S3Models.ObjectRef ref, Path cacheFile) throws IOException {
        ensureParentDir(cacheFile);

        // temp file in same dir for atomic move
        Path tmp = cacheFile.getParent().resolve(cacheFile.getFileName().toString() + ".tmp-" + UUID.randomUUID());

        long written = 0;
        try (InputStream in = delegate.getStream(ref)) {
            try (var out = Files.newOutputStream(tmp, CREATE, TRUNCATE_EXISTING, WRITE)) {
                written = in.transferTo(out);
            }
        } catch (Exception e) {
            Files.deleteIfExists(tmp);
            throw e;
        }

        evictIfNeeded(written);
        moveAtomically(tmp, cacheFile);

        currentCacheSize += written;
        metadataMap.put(cacheFile, new CacheMetadata());
        writeMeta(cacheFile, new CacheEntryMeta(ref.bucket(), ref.key(), written, java.time.Instant.now(), "S3"));
        logger.info("Cached from S3 to {} ({} bytes)", cacheFile, written);

    }

    private boolean tryPullFromPeers(S3Models.ObjectRef ref, Path cacheFile) {
        if (!peerEnabled || peerCacheClient == null || peerBaseUrls == null || peerBaseUrls.isEmpty()) return false;
        if (peerSecrets == null || peerSecrets.isEmpty()) return false;
        if (peerKeyId == null || peerKeyId.isBlank()) return false;
        String secret = peerSecrets.get(peerKeyId);
        if (secret == null || secret.isBlank()) return false;

        List<String> peers = new ArrayList<>(peerBaseUrls);
        Collections.shuffle(peers);

        int tries = 0;
        for (String peer : peers) {
            if (tries++ >= maxPeersToTry) break;

            String query = "bucket=" + ref.bucket() + "&key=" + ref.key();

            // HEAD
            PeerSignedHeaders headHdrs = PeerSignedHeaders.sign("HEAD", "/internal/cache/object", query, peerKeyId, secret);
            Optional<Long> len = peerCacheClient.headLength(
                    peer, ref.bucket(), ref.key(),
                    headHdrs, Duration.ofMillis(headTimeoutMs)
            );
            if (len.isEmpty()) continue;

            // GET stream -> tmp
            try {
                ensureParentDir(cacheFile);
                Path tmp = cacheFile.getParent().resolve(cacheFile.getFileName().toString() + ".tmp-" + UUID.randomUUID());

                PeerSignedHeaders getHdrs = PeerSignedHeaders.sign("GET", "/internal/cache/object", query, peerKeyId, secret);
                boolean ok = peerCacheClient.streamToFile(
                        peer, ref.bucket(), ref.key(),
                        getHdrs, tmp, Duration.ofMillis(getTimeoutMs)
                );

                if (!ok) {
                    Files.deleteIfExists(tmp);
                    continue;
                }

                long actual = Files.size(tmp);
                if (len.get() != null && len.get() > 0 && actual != len.get()) {
                    logger.warn("Peer transfer size mismatch from {} expected {} got {}", peer, len.get(), actual);
                    Files.deleteIfExists(tmp);
                    continue;
                }

                evictIfNeeded(actual);
                moveAtomically(tmp, cacheFile);

                currentCacheSize += actual;
                metadataMap.put(cacheFile, new CacheMetadata());
                writeMeta(cacheFile, new CacheEntryMeta(ref.bucket(), ref.key(), actual, java.time.Instant.now(), "PEER:" + peer));
                logger.info("Pulled {} bytes from peer {} into {}", actual, peer, cacheFile);
                return true;


            } catch (Exception e) {
                logger.warn("Peer transfer failed from {} for s3://{}/{}", peer, ref.bucket(), ref.key(), e);
            }
        }
        return false;
    }

    private Path cachePath(S3Models.ObjectRef ref) {
        String id = CacheKeyUtil.cacheId(ref.bucket(), ref.key());
        String shard = id.substring(0, 2);
        return cacheDir.resolve(shard).resolve(id + ".bin");
    }

    private void ensureParentDir(Path p) throws IOException {
        Path parent = p.getParent();
        if (parent != null) Files.createDirectories(parent);
    }

    private void moveAtomically(Path tmp, Path dest) throws IOException {
        try {
            Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
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
                    deleteMetaIfExists(fileToEvict);
                    currentCacheSize -= fileSize;
                    metadataMap.remove(fileToEvict);

                    metadataMap.remove(fileToEvict);
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
            return Files.walk(cacheDir)
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().contains(".tmp-"))
                    .filter(p -> p.getFileName().toString().endsWith(".bin"))
                    .sorted(Comparator.comparingLong(this::getFileLastModified))
                    .findFirst();
        } catch (IOException e) {
            logger.warn("Failed to list cache files for eviction", e);
            return Optional.empty();
        }
    }

    private long getFileLastModified(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }

    private long calculateCurrentCacheSize() {
        try {
            return Files.walk(cacheDir)
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().contains(".tmp-"))
                    .filter(p -> p.getFileName().toString().endsWith(".bin"))
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
            return 0;
        }
    }

    private Path metaPath(Path dataPath) {
        // .../xx/<id>.bin -> .../xx/<id>.meta
        String s = dataPath.toString();
        if (s.endsWith(".bin")) return Path.of(s.substring(0, s.length() - 4) + ".meta");
        return Path.of(s + ".meta");
    }

    private void writeMeta(Path cacheFile, CacheEntryMeta meta) throws IOException {
        Path mp = metaPath(cacheFile);
        Files.writeString(mp, CacheMetaCodec.encode(meta),
                StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING, WRITE);
    }

    private void deleteMetaIfExists(Path cacheFile) {
        try { Files.deleteIfExists(metaPath(cacheFile)); } catch (IOException ignore) {}
    }

    private void validateMetaOrWarn(S3Models.ObjectRef ref, Path cacheFile) {
        // Best-effort: if meta exists and bucket/key mismatch, warn (should not happen with hashing)
        Path mp = metaPath(cacheFile);
        if (!Files.exists(mp)) return;

        try {
            String line = Files.readString(mp, StandardCharsets.UTF_8);
            CacheEntryMeta m = CacheMetaCodec.decode(line);
            if (!ref.bucket().equals(m.bucket()) || !ref.key().equals(m.key())) {
                logger.warn("Cache meta mismatch for {}: meta says s3://{}/{}",
                        cacheFile, m.bucket(), m.key());
            }
        } catch (Exception e) {
            logger.debug("Failed to parse meta for {}", cacheFile, e);
        }
    }

    @Override
    public void delete(S3Models.ObjectRef ref) {
        delegate.delete(ref);
        Path cacheFile = cachePath(ref);
        try {
            if (Files.exists(cacheFile)) {
                long fileSize = Files.size(cacheFile);
                Files.deleteIfExists(cacheFile);
                deleteMetaIfExists(cacheFile);
                currentCacheSize -= fileSize;
                metadataMap.remove(cacheFile);
            }
            logger.info("Deleted cache file {} for s3://{}/{}", cacheFile, ref.bucket(), ref.key());
        } catch (IOException e) {
            logger.warn("Failed to delete cache file: {}", cacheFile, e);
        }
    }

    public Optional<CacheEntryMeta> readLocalMeta(S3Models.ObjectRef ref) {
        Path p = cachePath(ref);
        if (!Files.exists(p)) return Optional.empty();

        Path mp = metaPath(p);
        if (!Files.exists(mp)) return Optional.empty();

        try {
            String line = Files.readString(mp, java.nio.charset.StandardCharsets.UTF_8);
            return Optional.of(CacheMetaCodec.decode(line));
        } catch (Exception e) {
            logger.debug("Failed reading meta for {}", mp, e);
            return Optional.empty();
        }
    }

    // Delegate other methods unchanged

    @Override
    public String putBytes(S3Models.ObjectRef ref, byte[] bytes, String contentType, Map<String, String> userMetadata) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(bytes, "bytes");

        // 1) Commit to S3 first: local cache should mirror shared source of truth.
        String etag = delegate.putBytes(ref, bytes, contentType, userMetadata);

        // 2) Best-effort: warm local cache (do NOT fail the PUT if cache update fails).
        String lockKey = ref.bucket() + ":" + ref.key();
        Object lock = locks.computeIfAbsent(lockKey, k -> new Object());

        synchronized (lock) {
            Path cacheFile = cachePath(ref);
            try {
                ensureParentDir(cacheFile);

                long oldSize = 0;
                if (Files.exists(cacheFile)) {
                    try {
                        oldSize = Files.size(cacheFile);
                    } catch (IOException ignore) {
                        oldSize = 0;
                    }
                }

                // temp file in same dir for atomic-ish move
                Path tmp = cacheFile.getParent().resolve(cacheFile.getFileName().toString() + ".tmp-" + UUID.randomUUID());
                try {
                    Files.write(tmp, bytes, CREATE, TRUNCATE_EXISTING, WRITE);
                } catch (Exception e) {
                    Files.deleteIfExists(tmp);
                    throw e;
                }

                long newSize = bytes.length;
                long delta = newSize - oldSize;
                if (delta > 0) {
                    evictIfNeeded(delta);
                }

                moveAtomically(tmp, cacheFile);

                currentCacheSize += delta; // delta can be negative if overwriting with smaller
                metadataMap.put(cacheFile, new CacheMetadata());
                writeMeta(cacheFile, new CacheEntryMeta(ref.bucket(), ref.key(), newSize, java.time.Instant.now(), "PUT"));
                logger.info("Cached from PUT to {} ({} bytes) for s3://{}/{}", cacheFile, newSize, ref.bucket(), ref.key());
            } catch (Exception e) {
                logger.warn("S3 put succeeded but failed to update local cache for s3://{}/{}", ref.bucket(), ref.key(), e);
            }
        }

        return etag;
    }

    @Override
    public String putStream(S3Models.ObjectRef ref, InputStream in, long contentLength, String contentType, Map<String, String> userMetadata) {
        return delegate.putStream(ref, in, contentLength, contentType, userMetadata);
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
