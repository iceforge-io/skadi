package org.iceforge.skadi.semantic.cache;

/**
 * Abstract physical backend where a cache entry may be stored.
 *
 * <p>This enum is a descriptor — it identifies the kind of backend without
 * encoding connection details, credentials, or path structure. Physical
 * storage logic remains in {@code skadi-server}; this type exposes only
 * the category to boundary callers.
 *
 * <p>{@link #OBJECT_STORE} is reserved as a future extension seam for
 * non-S3 object stores (e.g., Azure Blob, GCS). It has no implementation
 * in Lane C.
 */
public enum CacheStorageBackend {

    /** In-process or local-filesystem cache (e.g. {@code QueryResultCache} in the gateway). */
    LOCAL,

    /** AWS S3 object store (e.g. {@code AwsSdkS3AccessLayer} in {@code skadi-server}). */
    S3,

    /**
     * A future object-store backend that is neither LOCAL nor S3.
     * Not implemented in Lane C — reserved as an extension seam.
     */
    OBJECT_STORE
}
