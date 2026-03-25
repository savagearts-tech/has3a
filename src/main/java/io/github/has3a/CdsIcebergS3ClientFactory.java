package io.github.has3a;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.glue.GlueClient;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Iceberg {@code AwsClientFactory} that delegates all S3 traffic to a
 * {@link SmartS3ClientProxy} backed by {@link BulkheadS3ClientFactory}.
 *
 * <h3>Supported Iceberg catalog properties</h3>
 * <table>
 *   <tr><th>Property</th><th>Default</th><th>Description</th></tr>
 *   <tr><td>{@code s3.endpoint.list}</td><td>http://minio1:9000,http://minio2:9000</td><td>Comma-separated MinIO node URLs</td></tr>
 *   <tr><td>{@code s3.access-key-id}</td><td><i>required</i></td><td>S3 access key</td></tr>
 *   <tr><td>{@code s3.secret-access-key}</td><td><i>required</i></td><td>S3 secret key</td></tr>
 *   <tr><td>{@code s3.region}</td><td>us-east-1</td><td>AWS region string</td></tr>
 *   <tr><td>{@code s3.pool.metadata.max-connections}</td><td>50</td><td>Metadata tier pool size per node</td></tr>
 *   <tr><td>{@code s3.pool.data.max-connections}</td><td>200</td><td>Data tier pool size per node</td></tr>
 *   <tr><td>{@code s3.pool.metadata.connection-timeout-ms}</td><td>2000</td><td>Metadata TCP connect timeout (ms)</td></tr>
 *   <tr><td>{@code s3.pool.metadata.socket-timeout-ms}</td><td>5000</td><td>Metadata socket timeout (ms)</td></tr>
 *   <tr><td>{@code s3.pool.metadata.acquisition-timeout-ms}</td><td>3000</td><td>Metadata connection acquisition timeout (ms)</td></tr>
 *   <tr><td>{@code s3.pool.data.connection-timeout-ms}</td><td>3000</td><td>Data TCP connect timeout (ms)</td></tr>
 *   <tr><td>{@code s3.pool.data.socket-timeout-ms}</td><td>60000</td><td>Data socket timeout (ms)</td></tr>
 *   <tr><td>{@code s3.pool.data.acquisition-timeout-ms}</td><td>5000</td><td>Data connection acquisition timeout (ms)</td></tr>
 *   <tr><td>{@code s3.proxy.quarantine-ttl-ms}</td><td>30000</td><td>Node quarantine duration (ms)</td></tr>
 *   <tr><td>{@code s3.proxy.multipart-route-idle-ttl-ms}</td><td>120000</td><td>Multipart upload sticky route idle timeout (ms)</td></tr>
 * </table>
 */
public class CdsIcebergS3ClientFactory implements org.apache.iceberg.aws.AwsClientFactory {
    private S3Client cachedProxyClient;

    @Override
    public void initialize(Map<String, String> properties) {
        String endpointsStr = properties.getOrDefault("s3.endpoint.list", "http://minio1:9000,http://minio2:9000");
        List<URI> endpoints = Arrays.stream(endpointsStr.split(","))
                .map(String::trim)
                .map(URI::create)
                .collect(Collectors.toList());

        // Fail fast at initialization time �?a missing credential surfaces as an opaque 403
        // at the first S3 call which is very hard to diagnose.
        String accessKey = Objects.requireNonNull(
                properties.get("s3.access-key-id"),
                "CdsIcebergS3ClientFactory: 's3.access-key-id' property is required");
        String secretKey = Objects.requireNonNull(
                properties.get("s3.secret-access-key"),
                "CdsIcebergS3ClientFactory: 's3.secret-access-key' property is required");

        String regionStr = properties.getOrDefault("s3.region", "us-east-1");

        BulkheadClientConfig config = parseConfig(properties);

        this.cachedProxyClient = BulkheadS3ClientFactory.createSmartProxy(
                endpoints, accessKey, secretKey, Region.of(regionStr),
                S3ClientMetrics.NO_OP, config);
    }

    @Override
    public S3Client s3() {
        return this.cachedProxyClient;
    }

    @Override
    public DynamoDbClient dynamo() {
        return null; // Not supported by this proxy factory
    }

    @Override
    public KmsClient kms() {
        return null; // Not supported by this proxy factory
    }

    @Override
    public GlueClient glue() {
        return null; // Not supported by this proxy factory
    }

    // ── Config parsing ────────────────────────────────────────────────────────

    private static BulkheadClientConfig parseConfig(Map<String, String> p) {
        return BulkheadClientConfig.builder()
                .metadataMaxConnections(
                        intProp(p, "s3.pool.metadata.max-connections",
                                BulkheadClientConfig.DEFAULT_METADATA_MAX_CONNECTIONS))
                .metadataConnectionTimeout(
                        durationMsProp(p, "s3.pool.metadata.connection-timeout-ms",
                                BulkheadClientConfig.DEFAULT_METADATA_CONNECTION_TIMEOUT))
                .metadataSocketTimeout(
                        durationMsProp(p, "s3.pool.metadata.socket-timeout-ms",
                                BulkheadClientConfig.DEFAULT_METADATA_SOCKET_TIMEOUT))
                .metadataConnectionAcquisitionTimeout(
                        durationMsProp(p, "s3.pool.metadata.acquisition-timeout-ms",
                                BulkheadClientConfig.DEFAULT_METADATA_CONNECTION_ACQUISITION_TIMEOUT))
                .dataMaxConnections(
                        intProp(p, "s3.pool.data.max-connections",
                                BulkheadClientConfig.DEFAULT_DATA_MAX_CONNECTIONS))
                .dataConnectionTimeout(
                        durationMsProp(p, "s3.pool.data.connection-timeout-ms",
                                BulkheadClientConfig.DEFAULT_DATA_CONNECTION_TIMEOUT))
                .dataSocketTimeout(
                        durationMsProp(p, "s3.pool.data.socket-timeout-ms",
                                BulkheadClientConfig.DEFAULT_DATA_SOCKET_TIMEOUT))
                .dataConnectionAcquisitionTimeout(
                        durationMsProp(p, "s3.pool.data.acquisition-timeout-ms",
                                BulkheadClientConfig.DEFAULT_DATA_CONNECTION_ACQUISITION_TIMEOUT))
                .quarantineTtlMillis(
                        longProp(p, "s3.proxy.quarantine-ttl-ms",
                                BulkheadClientConfig.DEFAULT_QUARANTINE_TTL_MILLIS))
                .multipartRouteIdleTtlMillis(
                        longProp(p, "s3.proxy.multipart-route-idle-ttl-ms",
                                BulkheadClientConfig.DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS))
                .build();
    }

    private static int intProp(Map<String, String> p, String key, int defaultVal) {
        String v = p.get(key);
        return v != null ? Integer.parseInt(v.trim()) : defaultVal;
    }

    private static long longProp(Map<String, String> p, String key, long defaultVal) {
        String v = p.get(key);
        return v != null ? Long.parseLong(v.trim()) : defaultVal;
    }

    private static Duration durationMsProp(Map<String, String> p, String key, Duration defaultVal) {
        String v = p.get(key);
        return v != null ? Duration.ofMillis(Long.parseLong(v.trim())) : defaultVal;
    }
}
