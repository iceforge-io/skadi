package com.dkay229.skadi.aws.s3;

import java.time.Instant;

public final class CacheMetaCodec {
    private CacheMetaCodec() {}

    public static String encode(CacheEntryMeta m) {
        // bucket \t key \t size \t cachedAtEpochMillis \t source
        return safe(m.bucket()) + "\t" +
                safe(m.key()) + "\t" +
                m.sizeBytes() + "\t" +
                m.cachedAt().toEpochMilli() + "\t" +
                safe(m.source()) + "\n";
    }

    public static CacheEntryMeta decode(String line) {
        String[] parts = line.strip().split("\t", 5);
        if (parts.length < 5) throw new IllegalArgumentException("Invalid meta line");
        return new CacheEntryMeta(
                uns(parts[0]),
                uns(parts[1]),
                Long.parseLong(parts[2]),
                Instant.ofEpochMilli(Long.parseLong(parts[3])),
                uns(parts[4])
        );
    }

    private static String safe(String s) {
        return (s == null) ? "" : s.replace("\t", " ").replace("\n", " ");
    }

    private static String uns(String s) {
        return (s == null) ? "" : s;
    }
}
