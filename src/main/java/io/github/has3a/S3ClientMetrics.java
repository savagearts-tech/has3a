package io.github.has3a;

/**
 * SPI for recording S3 operation metrics.
 * <p>
 * Implement this interface and pass it to {@link BulkheadS3ClientFactory#createSmartProxy}
 * to hook in any metrics backend (Micrometer, Prometheus, etc.) without adding a
 * compile-time dependency to this library.
 * <p>
 * A built-in {@link #NO_OP} constant is provided for callers that do not need metrics.
 */
public interface S3ClientMetrics {

    /** Bulkhead tier that handled the call. */
    enum Tier { METADATA, DATA }

    /** Final outcome of a single call attempt. */
    enum Outcome { SUCCESS, FAILOVER, ERROR }

    /**
     * Immutable record of a single S3 call attempt.
     *
     * @param operation   S3 method name, e.g. {@code "putObject"}
     * @param tier        which connection-pool tier was used
     * @param nodeIndex   zero-based index of the selected MinIO node
     * @param durationNs  wall-clock duration of the attempt in nanoseconds
     * @param outcome     result of this attempt
     */
    record CallRecord(
            String  operation,
            Tier    tier,
            int     nodeIndex,
            long    durationNs,
            Outcome outcome
    ) {}

    /**
     * Called exactly once per attempt (successful or failed).
     *
     * @param record details of the call attempt
     */
    void record(CallRecord record);

    /** No-op implementation �?safe default when no metrics backend is configured. */
    S3ClientMetrics NO_OP = record -> {};
}
