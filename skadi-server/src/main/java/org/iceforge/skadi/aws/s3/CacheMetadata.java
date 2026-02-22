package org.iceforge.skadi.aws.s3;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CacheMetadata {
    private final Instant creationTime;
    private final List<Instant> accessTimes;

    public CacheMetadata() {
        this.creationTime = Instant.now();
        this.accessTimes = new ArrayList<>();
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public synchronized void addAccessTime() {
        accessTimes.add(Instant.now());
    }

    public synchronized List<Instant> getAccessTimes() {
        return new ArrayList<>(accessTimes);
    }
}