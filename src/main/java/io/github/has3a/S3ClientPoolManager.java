package io.github.has3a;

/**
 * Management interface for S3Client connection pools.
 * Allows programmatic intervention to recover from pool-level issues (e.g. stalled connections)
 * without restarting the application.
 */
public interface S3ClientPoolManager {
    
    /**
     * Gracefully tears down the existing connection pools and instantiates new ones.
     * In-flight requests on the old pools may be interrupted or completed depending on the 
     * specific underlying HTTP client behavior.
     */
    void forceRebuildPools();

}
