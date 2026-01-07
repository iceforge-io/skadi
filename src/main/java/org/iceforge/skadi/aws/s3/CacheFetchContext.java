package org.iceforge.skadi.aws.s3;

public final class CacheFetchContext {
    private CacheFetchContext() {}

    public enum Source { LOCAL, PEER, S3, UNKNOWN }

    private static final ThreadLocal<Source> TL =
            ThreadLocal.withInitial(() -> Source.UNKNOWN);

    public static void set(Source s) {
        TL.set(s == null ? Source.UNKNOWN : s);
    }

    /** Read then clear to avoid leaking across reused request threads. */
    public static Source getAndClear() {
        Source s = TL.get();
        TL.remove();
        return s;
    }
}
