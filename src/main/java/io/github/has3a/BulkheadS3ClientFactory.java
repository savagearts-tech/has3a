package io.github.has3a;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class BulkheadS3ClientFactory {

    // ── Public factory overloads ──────────────────────────────────────────────

    /**
     * Creates a {@link SmartS3ClientProxy} with default connection-pool settings.
     * Equivalent to {@code createSmartProxy(endpoints, accessKey, secretKey, region, BulkheadClientConfig.defaults())}.
     */
    public static S3Client createSmartProxy(
            List<URI> endpoints, String accessKey, String secretKey, Region region) {
        return createSmartProxy(endpoints, accessKey, secretKey, region,
                S3ClientMetrics.NO_OP, BulkheadClientConfig.defaults());
    }

    /**
     * Creates a {@link SmartS3ClientProxy} with a custom metrics sink and default connection-pool settings.
     */
    public static S3Client createSmartProxy(
            List<URI> endpoints, String accessKey, String secretKey, Region region,
            S3ClientMetrics metrics) {
        return createSmartProxy(endpoints, accessKey, secretKey, region,
                metrics, BulkheadClientConfig.defaults());
    }

    /**
     * Creates a {@link SmartS3ClientProxy} with fully customised connection-pool settings.
     *
     * @param endpoints   ordered list of MinIO node URIs
     * @param accessKey   S3 access key
     * @param secretKey   S3 secret key
     * @param region      AWS region (MinIO typically uses {@code us-east-1})
     * @param metrics     observability sink; pass {@link S3ClientMetrics#NO_OP} to disable
     * @param config      connection-pool and proxy tuning parameters
     */
    public static S3Client createSmartProxy(
            List<URI> endpoints, String accessKey, String secretKey, Region region,
            S3ClientMetrics metrics, BulkheadClientConfig config) {

        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build();

        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey));

        Supplier<SmartS3ClientProxy.Clients> clientSupplier = () -> {
            List<S3Client> metadataClients = new ArrayList<>();
            List<S3Client> dataClients     = new ArrayList<>();

            for (URI endpoint : endpoints) {
                SdkHttpClient metadataHttpClient = buildApacheHttpClient(
                        config.metadataMaxConnections(),
                        config.metadataConnectionTimeout(),
                        config.metadataSocketTimeout(),
                        config.metadataConnectionAcquisitionTimeout(),
                        true // Metadata always defaults to true
                );

                SdkHttpClient dataHttpClient = buildApacheHttpClient(
                        config.dataMaxConnections(),
                        config.dataConnectionTimeout(),
                        config.dataSocketTimeout(),
                        config.dataConnectionAcquisitionTimeout(),
                        config.dataExpectContinueEnabled()
                );

                S3Client metadataClient = S3Client.builder()
                        .endpointOverride(endpoint)
                        .region(region)
                        .credentialsProvider(credentialsProvider)
                        .serviceConfiguration(s3Config)
                        .httpClient(metadataHttpClient)
                        .build();

                software.amazon.awssdk.services.s3.S3ClientBuilder dataClientBuilder = S3Client.builder()
                        .endpointOverride(endpoint)
                        .region(region)
                        .credentialsProvider(credentialsProvider)
                        .serviceConfiguration(s3Config)
                        .httpClient(dataHttpClient);
                        
                if (config.dataApiCallTimeout() != null) {
                    dataClientBuilder.overrideConfiguration(c -> c.apiCallTimeout(config.dataApiCallTimeout()));
                }
                
                S3Client dataClient = dataClientBuilder.build();

                metadataClients.add(metadataClient);
                dataClients.add(dataClient);
            }
            return new SmartS3ClientProxy.Clients(metadataClients, dataClients);
        };

        SmartS3ClientProxy.Clients initialClients = clientSupplier.get();

        return SmartS3ClientProxy.create(
                initialClients.metadataClients, initialClients.dataClients, endpoints,
                metrics, config.quarantineTtlMillis(),
                config.multipartRouteIdleTtlMillis(),
                clientSupplier);
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /**
     * Builds an {@link SdkHttpClient} backed by Apache HTTP Client.
     *
     * <p>Key design decisions:
     * <ul>
     *   <li><b>maxConnections</b>: Controls the HTTP connection pool size for this bulkhead tier.</li>
     *   <li><b>connectionTimeout</b>: TCP handshake timeout �?kept short to detect unreachable nodes fast.</li>
     *   <li><b>socketTimeout</b>: Idle read/write timeout �?data operations need longer windows for
     *       large payloads; metadata operations are capped tightly to fail fast on stalled responses.</li>
     *   <li><b>connectionAcquisitionTimeout</b>: Maximum time to wait for a connection from the pool
     *       before failing �?prevents head-of-line blocking under high concurrency.</li>
     *   <li><b>tcpKeepAlive=true</b>: Enables TCP keep-alive to avoid silent connection drops by
     *       network appliances (NAT gateways, load balancers) between client and MinIO nodes.</li>
     * </ul>
     */
    private static SdkHttpClient buildApacheHttpClient(
            int      maxConnections,
            Duration connectionTimeout,
            Duration socketTimeout,
            Duration connectionAcquisitionTimeout,
            boolean  expectContinueEnabled) {

        return ApacheHttpClient.builder()
                .maxConnections(maxConnections)
                .connectionTimeout(connectionTimeout)
                .socketTimeout(socketTimeout)
                .connectionAcquisitionTimeout(connectionAcquisitionTimeout)
                .expectContinueEnabled(expectContinueEnabled)
                .tcpKeepAlive(true)
                .build();
    }
}
