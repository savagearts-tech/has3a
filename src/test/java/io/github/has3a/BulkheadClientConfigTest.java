package io.github.has3a;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BulkheadClientConfigTest {

    // -------------------------------------------------------------------------
    // 1. Default values
    // -------------------------------------------------------------------------

    @Test
    void defaults_matchExpectedHardCodedValues() {
        BulkheadClientConfig cfg = BulkheadClientConfig.defaults();

        assertEquals(50,  cfg.metadataMaxConnections());
        assertEquals(Duration.ofSeconds(2),  cfg.metadataConnectionTimeout());
        assertEquals(Duration.ofSeconds(5),  cfg.metadataSocketTimeout());
        assertEquals(Duration.ofSeconds(3),  cfg.metadataConnectionAcquisitionTimeout());

        assertEquals(200, cfg.dataMaxConnections());
        assertEquals(Duration.ofSeconds(3),  cfg.dataConnectionTimeout());
        assertEquals(Duration.ofSeconds(60), cfg.dataSocketTimeout());
        assertEquals(Duration.ofSeconds(5),  cfg.dataConnectionAcquisitionTimeout());

        assertEquals(30_000L, cfg.quarantineTtlMillis());
        assertEquals(BulkheadClientConfig.DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS, cfg.multipartRouteIdleTtlMillis());
    }

    @Test
    void builderDefaults_sameAsDefaults() {
        BulkheadClientConfig fromBuilder  = BulkheadClientConfig.builder().build();
        BulkheadClientConfig fromDefaults = BulkheadClientConfig.defaults();

        assertEquals(fromDefaults.metadataMaxConnections(),               fromBuilder.metadataMaxConnections());
        assertEquals(fromDefaults.metadataConnectionTimeout(),            fromBuilder.metadataConnectionTimeout());
        assertEquals(fromDefaults.metadataSocketTimeout(),                fromBuilder.metadataSocketTimeout());
        assertEquals(fromDefaults.metadataConnectionAcquisitionTimeout(), fromBuilder.metadataConnectionAcquisitionTimeout());
        assertEquals(fromDefaults.dataMaxConnections(),                   fromBuilder.dataMaxConnections());
        assertEquals(fromDefaults.dataConnectionTimeout(),                fromBuilder.dataConnectionTimeout());
        assertEquals(fromDefaults.dataSocketTimeout(),                    fromBuilder.dataSocketTimeout());
        assertEquals(fromDefaults.dataConnectionAcquisitionTimeout(),     fromBuilder.dataConnectionAcquisitionTimeout());
        assertEquals(fromDefaults.quarantineTtlMillis(),                  fromBuilder.quarantineTtlMillis());
        assertEquals(fromDefaults.multipartRouteIdleTtlMillis(),          fromBuilder.multipartRouteIdleTtlMillis());
    }

    // -------------------------------------------------------------------------
    // 2. Custom values round-trip through builder
    // -------------------------------------------------------------------------

    @Test
    void builder_customValues_reflected() {
        BulkheadClientConfig cfg = BulkheadClientConfig.builder()
                .metadataMaxConnections(10)
                .metadataConnectionTimeout(Duration.ofMillis(500))
                .metadataSocketTimeout(Duration.ofMillis(1000))
                .metadataConnectionAcquisitionTimeout(Duration.ofMillis(750))
                .dataMaxConnections(400)
                .dataConnectionTimeout(Duration.ofSeconds(5))
                .dataSocketTimeout(Duration.ofSeconds(120))
                .dataConnectionAcquisitionTimeout(Duration.ofSeconds(10))
                .quarantineTtlMillis(5_000L)
                .multipartRouteIdleTtlMillis(120_000L)
                .build();

        assertEquals(10,  cfg.metadataMaxConnections());
        assertEquals(Duration.ofMillis(500),  cfg.metadataConnectionTimeout());
        assertEquals(Duration.ofMillis(1000), cfg.metadataSocketTimeout());
        assertEquals(Duration.ofMillis(750),  cfg.metadataConnectionAcquisitionTimeout());

        assertEquals(400, cfg.dataMaxConnections());
        assertEquals(Duration.ofSeconds(5),   cfg.dataConnectionTimeout());
        assertEquals(Duration.ofSeconds(120), cfg.dataSocketTimeout());
        assertEquals(Duration.ofSeconds(10),  cfg.dataConnectionAcquisitionTimeout());

        assertEquals(5_000L, cfg.quarantineTtlMillis());
        assertEquals(120_000L, cfg.multipartRouteIdleTtlMillis());
    }

    @Test
    void builder_partialOverride_leavesOthersAtDefault() {
        BulkheadClientConfig cfg = BulkheadClientConfig.builder()
                .dataMaxConnections(300)
                .build();

        // overridden
        assertEquals(300, cfg.dataMaxConnections());
        // still default
        assertEquals(BulkheadClientConfig.DEFAULT_METADATA_MAX_CONNECTIONS, cfg.metadataMaxConnections());
        assertEquals(BulkheadClientConfig.DEFAULT_QUARANTINE_TTL_MILLIS,    cfg.quarantineTtlMillis());
    }

    // -------------------------------------------------------------------------
    // 3. quarantineTtlMillis flows through SmartS3ClientProxy
    // -------------------------------------------------------------------------

    @Test
    void customQuarantineTtl_usedByProxy_shortTtlAllowsNodeRecovery() throws InterruptedException {
        S3Client failNode    = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);

        // failNode throws once (triggers quarantine), then succeeds
        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build())
                .thenReturn(null);

        // 1-second quarantine TTL so recovery is observable in a unit test
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(failNode, successNode),
                List.of(mock(S3Client.class), mock(S3Client.class)),
                List.of(URI.create("http://node1"), URI.create("http://node2")),
                S3ClientMetrics.NO_OP,
                /* quarantineTtlMillis */ 1_000L
        );

        // First call: failNode quarantined, failover to successNode
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        ProxyStats statsAfterFirst = SmartS3ClientProxy.handlerOf(proxy).getStats();
        assertTrue(statsAfterFirst.nodeStats().get(0).quarantined(), "node 0 should be quarantined");

        // Wait for TTL to expire
        Thread.sleep(1_100);

        // After TTL: failNode should be eligible again
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        ProxyStats statsAfterRecovery = SmartS3ClientProxy.handlerOf(proxy).getStats();
        assertFalse(statsAfterRecovery.nodeStats().get(0).quarantined(),
                "node 0 should have recovered after quarantine TTL");
    }

    // -------------------------------------------------------------------------
    // 4. CdsIcebergS3ClientFactory property parsing
    // -------------------------------------------------------------------------

    @Test
    void icebergFactory_parseConfig_customPoolSize() {
        // We verify the property-parsing helpers in isolation via reflection-free
        // black-box: initialize with custom properties and confirm no exception.
        // (Full integration with live MinIO is covered by IcebergS3FileIOCatalogIntegrationTest.)
        java.util.Map<String, String> props = new java.util.HashMap<>();
        props.put("s3.access-key-id",                       "minioadmin");
        props.put("s3.secret-access-key",                   "minioadmin");
        props.put("s3.endpoint.list",                       "http://localhost:9000");
        props.put("s3.pool.metadata.max-connections",       "25");
        props.put("s3.pool.data.max-connections",           "100");
        props.put("s3.pool.metadata.connection-timeout-ms", "1000");
        props.put("s3.pool.data.socket-timeout-ms",         "30000");
        props.put("s3.proxy.quarantine-ttl-ms",             "15000");
        props.put("s3.proxy.multipart-route-idle-ttl-ms",   "7200000");

        assertDoesNotThrow(() -> {
            CdsIcebergS3ClientFactory factory = new CdsIcebergS3ClientFactory();
            factory.initialize(props);
        });
    }

    @Test
    void icebergFactory_missingRequiredCredential_throwsNPE() {
        java.util.Map<String, String> props = new java.util.HashMap<>();
        props.put("s3.endpoint.list", "http://localhost:9000");
        // access-key-id intentionally omitted

        assertThrows(NullPointerException.class, () -> {
            CdsIcebergS3ClientFactory factory = new CdsIcebergS3ClientFactory();
            factory.initialize(props);
        });
    }

    // -------------------------------------------------------------------------
    // 5. Recommended configuration
    //
    //   Suitable for: multi-node MinIO cluster (3+ nodes), target throughput >900 MB/s.
    //   - data pool expanded to 400 to utilize multi-core NIC bandwidth
    //   - quarantine TTL shortened to 10s for faster recovery from transient failures
    // -------------------------------------------------------------------------

    /**
     * Verifies that the recommended Java Builder API configuration loads correctly.
     *
     * Recommended usage:
     * <pre>{@code
     * BulkheadClientConfig config = BulkheadClientConfig.builder()
     *         .dataMaxConnections(400)
     *         .quarantineTtlMillis(10_000)
     *         .build();
     *
     * S3Client client = BulkheadS3ClientFactory.createSmartProxy(
     *         endpoints, accessKey, secretKey, region, metrics, config);
     * }</pre>
     */
    @Test
    void recommendedConfig_javaApi_parametersReflected() {
        BulkheadClientConfig config = BulkheadClientConfig.builder()
                .dataMaxConnections(400)
                .quarantineTtlMillis(10_000)
                .build();

        // Overridden values
        assertEquals(400,    config.dataMaxConnections(),  "data pool should be 400");
        assertEquals(10_000, config.quarantineTtlMillis(), "quarantine TTL should be 10s");

        // Non-overridden params must remain at defaults (no accidental zero-out)
        assertEquals(BulkheadClientConfig.DEFAULT_METADATA_MAX_CONNECTIONS,
                config.metadataMaxConnections(), "metadata pool should stay at default");
        assertEquals(BulkheadClientConfig.DEFAULT_DATA_SOCKET_TIMEOUT,
                config.dataSocketTimeout(), "data socket timeout should stay at default");
    }

    /**
     * Verifies that the recommended Iceberg catalog properties configuration is accepted
     * by {@link CdsIcebergS3ClientFactory} without error.
     *
     * Recommended usage:
     * <pre>{@code
     * properties.put("s3.pool.data.max-connections", "400");
     * properties.put("s3.proxy.quarantine-ttl-ms",   "10000");
     * }</pre>
     */
    @Test
    void recommendedConfig_icebergProperties_parsedWithoutError() {
        java.util.Map<String, String> props = new java.util.HashMap<>();
        // Required credentials
        props.put("s3.access-key-id",     "minioadmin");
        props.put("s3.secret-access-key", "minioadmin");
        props.put("s3.endpoint.list",     "http://localhost:9000");

        // Recommended connection pool settings
        props.put("s3.pool.data.max-connections", "400");   // larger data pool for high throughput
        props.put("s3.proxy.quarantine-ttl-ms",   "10000"); // 10s quarantine window

        assertDoesNotThrow(() -> {
            CdsIcebergS3ClientFactory factory = new CdsIcebergS3ClientFactory();
            factory.initialize(props);
        }, "Recommended config should be parsed and initialized without error");
    }
}
