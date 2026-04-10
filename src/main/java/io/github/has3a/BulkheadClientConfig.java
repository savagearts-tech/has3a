package io.github.has3a;

import java.time.Duration;

/**
 * Immutable configuration for the HTTP connection pools and proxy behaviour of
 * {@link BulkheadS3ClientFactory}.
 *
 * <p>Two independent Apache HTTP connection pools are maintained per MinIO endpoint:
 * <ul>
 *   <li><b>Metadata tier</b> low-latency pool for small, short-lived operations
 *       (list, head, createMultipartUpload, etc.)</li>
 *   <li><b>Data tier</b> �?high-throughput pool for bulk transfers
 *       (put, get, uploadPart, etc.)</li>
 * </ul>
 *
 * <p>Use the nested {@link Builder} to override individual defaults:
 * <pre>{@code
 * BulkheadClientConfig config = BulkheadClientConfig.builder()
 *         .dataMaxConnections(400)
 *         .quarantineTtlMillis(10_000)
 *         .build();
 * }</pre>
 *
 * <p>All defaults match the previous hard-coded values so existing callers experience
 * no behavioural change after the upgrade.
 */
public final class BulkheadClientConfig {

    // ── Metadata tier defaults ────────────────────────────────────────────────
    /** Default per-node connection pool size for metadata operations. */
    public static final int      DEFAULT_METADATA_MAX_CONNECTIONS              = 50;
    /** Default TCP connect timeout for metadata operations. */
    public static final Duration DEFAULT_METADATA_CONNECTION_TIMEOUT           = Duration.ofSeconds(2);
    /** Default socket (read/write) timeout for metadata operations. */
    public static final Duration DEFAULT_METADATA_SOCKET_TIMEOUT               = Duration.ofSeconds(5);
    /** Default max wait time to acquire a connection from the metadata pool. */
    public static final Duration DEFAULT_METADATA_CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(3);

    // ── Data tier defaults ────────────────────────────────────────────────────
    /**
     * Default per-node connection pool size for data operations.
     * Capped at 200: MinIO's accept-queue is ~1024 total; a larger value per client
     * would cause ECONNREFUSED under sustained multi-client load.
     */
    public static final int      DEFAULT_DATA_MAX_CONNECTIONS                  = 200;
    /** Default TCP connect timeout for data operations. */
    public static final Duration DEFAULT_DATA_CONNECTION_TIMEOUT               = Duration.ofSeconds(3);
    /** Default socket (read/write) timeout for data operations. */
    public static final Duration DEFAULT_DATA_SOCKET_TIMEOUT                   = Duration.ofSeconds(60);
    /** Default max wait time to acquire a connection from the data pool. */
    public static final Duration DEFAULT_DATA_CONNECTION_ACQUISITION_TIMEOUT   = Duration.ofSeconds(5);
    /** Default HTTP Expect: 100-continue behavior for data PUT operations. */
    public static final boolean  DEFAULT_DATA_EXPECT_CONTINUE_ENABLED          = true;
    /** Default SDK level total API call timeout (null means disabled/infinite). */
    public static final Duration DEFAULT_DATA_API_CALL_TIMEOUT                 = null;

    // ── Proxy defaults ────────────────────────────────────────────────────────
    /**
     * Default node quarantine TTL in milliseconds.
     * A node that throws a network-layer failure is excluded from routing for this duration.
     */
    public static final long DEFAULT_QUARANTINE_TTL_MILLIS = 30_000L;

    /**
     * Default duration for which a multipart upload route remains sticky.
     * UploadPart operations matching the uploadId will be routed to the same node
     * that processed the CreateMultipartUpload.
     */
    public static final long DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS = 120_000L;

    // ── Fields ────────────────────────────────────────────────────────────────

    private final int      metadataMaxConnections;
    private final Duration metadataConnectionTimeout;
    private final Duration metadataSocketTimeout;
    private final Duration metadataConnectionAcquisitionTimeout;

    private final int      dataMaxConnections;
    private final Duration dataConnectionTimeout;
    private final Duration dataSocketTimeout;
    private final Duration dataConnectionAcquisitionTimeout;
    private final boolean  dataExpectContinueEnabled;
    private final Duration dataApiCallTimeout;

    private final long quarantineTtlMillis;
    private final long multipartRouteIdleTtlMillis;

    // ── Construction ─────────────────────────────────────────────────────────

    private BulkheadClientConfig(Builder b) {
        this.metadataMaxConnections                 = b.metadataMaxConnections;
        this.metadataConnectionTimeout              = b.metadataConnectionTimeout;
        this.metadataSocketTimeout                  = b.metadataSocketTimeout;
        this.metadataConnectionAcquisitionTimeout   = b.metadataConnectionAcquisitionTimeout;
        this.dataMaxConnections                     = b.dataMaxConnections;
        this.dataConnectionTimeout                  = b.dataConnectionTimeout;
        this.dataSocketTimeout                      = b.dataSocketTimeout;
        this.dataConnectionAcquisitionTimeout       = b.dataConnectionAcquisitionTimeout;
        this.dataExpectContinueEnabled              = b.dataExpectContinueEnabled;
        this.dataApiCallTimeout                     = b.dataApiCallTimeout;
        this.quarantineTtlMillis                    = b.quarantineTtlMillis;
        this.multipartRouteIdleTtlMillis            = b.multipartRouteIdleTtlMillis;
    }

    /** Returns a new builder pre-populated with all default values. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns an instance that carries all default values (equivalent to {@code builder().build()}). */
    public static BulkheadClientConfig defaults() {
        return builder().build();
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public int      metadataMaxConnections()                 { return metadataMaxConnections; }
    public Duration metadataConnectionTimeout()              { return metadataConnectionTimeout; }
    public Duration metadataSocketTimeout()                  { return metadataSocketTimeout; }
    public Duration metadataConnectionAcquisitionTimeout()   { return metadataConnectionAcquisitionTimeout; }

    public int      dataMaxConnections()                     { return dataMaxConnections; }
    public Duration dataConnectionTimeout()                  { return dataConnectionTimeout; }
    public Duration dataSocketTimeout()                      { return dataSocketTimeout; }
    public Duration dataConnectionAcquisitionTimeout()       { return dataConnectionAcquisitionTimeout; }
    public boolean  dataExpectContinueEnabled()              { return dataExpectContinueEnabled; }
    public Duration dataApiCallTimeout()                     { return dataApiCallTimeout; }

    public long     quarantineTtlMillis()                    { return quarantineTtlMillis; }
    public long     multipartRouteIdleTtlMillis()            { return multipartRouteIdleTtlMillis; }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {

        private int      metadataMaxConnections               = DEFAULT_METADATA_MAX_CONNECTIONS;
        private Duration metadataConnectionTimeout            = DEFAULT_METADATA_CONNECTION_TIMEOUT;
        private Duration metadataSocketTimeout                = DEFAULT_METADATA_SOCKET_TIMEOUT;
        private Duration metadataConnectionAcquisitionTimeout = DEFAULT_METADATA_CONNECTION_ACQUISITION_TIMEOUT;

        private int      dataMaxConnections                   = DEFAULT_DATA_MAX_CONNECTIONS;
        private Duration dataConnectionTimeout                = DEFAULT_DATA_CONNECTION_TIMEOUT;
        private Duration dataSocketTimeout                    = DEFAULT_DATA_SOCKET_TIMEOUT;
        private Duration dataConnectionAcquisitionTimeout     = DEFAULT_DATA_CONNECTION_ACQUISITION_TIMEOUT;
        private boolean  dataExpectContinueEnabled            = DEFAULT_DATA_EXPECT_CONTINUE_ENABLED;
        private Duration dataApiCallTimeout                   = DEFAULT_DATA_API_CALL_TIMEOUT;

        private long quarantineTtlMillis         = DEFAULT_QUARANTINE_TTL_MILLIS;
        private long multipartRouteIdleTtlMillis = DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS;

        private Builder() {}

        /** Maximum simultaneous connections in the per-node <b>metadata</b> pool. */
        public Builder metadataMaxConnections(int v)                 { metadataMaxConnections = v;               return this; }
        /** TCP connect timeout for <b>metadata</b> connections. */
        public Builder metadataConnectionTimeout(Duration v)         { metadataConnectionTimeout = v;            return this; }
        /** Socket read/write timeout for <b>metadata</b> connections. */
        public Builder metadataSocketTimeout(Duration v)             { metadataSocketTimeout = v;                return this; }
        /** Maximum wait to acquire a connection from the <b>metadata</b> pool. */
        public Builder metadataConnectionAcquisitionTimeout(Duration v) { metadataConnectionAcquisitionTimeout = v; return this; }

        /** Maximum simultaneous connections in the per-node <b>data</b> pool. */
        public Builder dataMaxConnections(int v)                     { dataMaxConnections = v;                   return this; }
        /** TCP connect timeout for <b>data</b> connections. */
        public Builder dataConnectionTimeout(Duration v)             { dataConnectionTimeout = v;                return this; }
        /** Socket read/write timeout for <b>data</b> connections. */
        public Builder dataSocketTimeout(Duration v)                 { dataSocketTimeout = v;                    return this; }
        /** Maximum wait to acquire a connection from the <b>data</b> pool. */
        public Builder dataConnectionAcquisitionTimeout(Duration v)  { dataConnectionAcquisitionTimeout = v;     return this; }
        /** Set whether to enable HTTP Expect: 100-continue for large payloads on data pool. */
        public Builder dataExpectContinueEnabled(boolean v)          { dataExpectContinueEnabled = v;            return this; }
        /** Set an overall max timeout for an S3 API call over the data pool. Null disables the limit. */
        public Builder dataApiCallTimeout(Duration v)                { dataApiCallTimeout = v;                   return this; }

        /**
         * Duration in milliseconds for which a node is excluded from routing after a network failure.
         * Defaults to {@value BulkheadClientConfig#DEFAULT_QUARANTINE_TTL_MILLIS} ms.
         */
        public Builder quarantineTtlMillis(long v)                   { quarantineTtlMillis = v;                  return this; }

        /**
         * Duration in milliseconds for which a multipart upload route is kept alive.
         * Defaults to {@value BulkheadClientConfig#DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS} ms.
         */
        public Builder multipartRouteIdleTtlMillis(long v)           { multipartRouteIdleTtlMillis = v;          return this; }

        public BulkheadClientConfig build() {
            return new BulkheadClientConfig(this);
        }
    }
}
