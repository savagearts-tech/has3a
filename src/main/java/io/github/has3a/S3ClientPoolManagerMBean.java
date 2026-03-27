package io.github.has3a;

/**
 * JMX MBean interface for S3Client pool management.
 */
public interface S3ClientPoolManagerMBean {

    /**
     * Tearing down and rebuilding old connection pools.
     */
    void forceRebuildPools();

    /**
     * @return The number of times the connection pool has been rebuilt.
     */
    long getPoolRebuildCount();

    /**
     * @return The ISO-8601 timestamp of the last pool rebuild, or "N/A" if never rebuilt.
     */
    String getLastRebuildTime();

}
