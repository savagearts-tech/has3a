package io.github.has3a;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Iceberg {@link org.apache.iceberg.aws.s3.S3FileIO} + {@link CdsIcebergS3ClientFactory} against
 * MinIO, without Hadoop {@code s3a://} or S3A configuration. Metastore is {@link JdbcCatalog} (H2).
 */
public class IcebergS3FileIOCatalogIntegrationTest {

    private static void putBulkheadClientConfig(Map<String, String> properties, BulkheadClientConfig config) {
        properties.put("s3.pool.metadata.max-connections", Integer.toString(config.metadataMaxConnections()));
        properties.put("s3.pool.metadata.connection-timeout-ms",
                Long.toString(config.metadataConnectionTimeout().toMillis()));
        properties.put("s3.pool.metadata.socket-timeout-ms",
                Long.toString(config.metadataSocketTimeout().toMillis()));
        properties.put("s3.pool.metadata.acquisition-timeout-ms",
                Long.toString(config.metadataConnectionAcquisitionTimeout().toMillis()));
        properties.put("s3.pool.data.max-connections", Integer.toString(config.dataMaxConnections()));
        properties.put("s3.pool.data.connection-timeout-ms",
                Long.toString(config.dataConnectionTimeout().toMillis()));
        properties.put("s3.pool.data.socket-timeout-ms",
                Long.toString(config.dataSocketTimeout().toMillis()));
        properties.put("s3.pool.data.acquisition-timeout-ms",
                Long.toString(config.dataConnectionAcquisitionTimeout().toMillis()));
        properties.put("s3.proxy.quarantine-ttl-ms", Long.toString(config.quarantineTtlMillis()));
    }

    @Test
    public void testJdbcCatalogS3FileIOReadWrite() throws IOException {
        // Parquet codecs load Hadoop Shell on Windows; dummy home avoids HADOOP_HOME errors (unrelated to s3a://).
        java.io.File dummyHadoop = new java.io.File(System.getProperty("java.io.tmpdir"), "hadoop-dummy");
        java.io.File binDir = new java.io.File(dummyHadoop, "bin");
        binDir.mkdirs();
        new java.io.File(binDir, "winutils.exe").createNewFile();
        System.setProperty("hadoop.home.dir", dummyHadoop.getAbsolutePath());

        String warehousePath = "s3://integration-test-bucket/warehouse";

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "jdbc:h2:mem:iceberg_has3a;DB_CLOSE_DELAY=-1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);

        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("client.factory", "io.github.has3a.CdsIcebergS3ClientFactory");
        properties.put("s3.endpoint.list", "http://127.0.0.1:9000");
        properties.put("s3.access-key-id", "minioadmin");
        properties.put("s3.secret-access-key", "minioadmin");
        properties.put("s3.path-style-access", "true");

        Path s3Staging = Path.of(System.getProperty("user.dir"), "target", "has3a-s3fileio-staging");
        Files.createDirectories(s3Staging);
        properties.put(IcebergS3FileIOStaging.HAS3A_STAGING_CATALOG_KEY, s3Staging.toString());
        IcebergS3FileIOStaging.applyPreferredStagingDirectory(properties);

        BulkheadClientConfig proxyConfig = BulkheadClientConfig.builder()
                .metadataMaxConnections(50)
                .metadataConnectionTimeout(Duration.ofSeconds(2))
                .metadataSocketTimeout(Duration.ofSeconds(5))
                .metadataConnectionAcquisitionTimeout(Duration.ofSeconds(3))
                .dataMaxConnections(400)
                .dataConnectionTimeout(Duration.ofSeconds(3))
                .dataSocketTimeout(Duration.ofSeconds(60))
                .dataConnectionAcquisitionTimeout(Duration.ofSeconds(5))
                .quarantineTtlMillis(10_000L)
                .build();
        putBulkheadClientConfig(properties, proxyConfig);

        try (S3Client setupClient = BulkheadS3ClientFactory.createSmartProxy(
                Collections.singletonList(URI.create("http://127.0.0.1:9000")),
                "minioadmin",
                "minioadmin",
                Region.US_EAST_1,
                S3ClientMetrics.NO_OP,
                proxyConfig)) {
            try {
                setupClient.createBucket(CreateBucketRequest.builder().bucket("integration-test-bucket").build());
            } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
            }
        }

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("jdbc_minio_catalog", properties);

        try {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "data", Types.StringType.get())
            );
            PartitionSpec spec = PartitionSpec.unpartitioned();
            Namespace ns = Namespace.of("default");
            TableIdentifier name = TableIdentifier.of(ns, "sample_table");

            if (!catalog.namespaceExists(ns)) {
                catalog.createNamespace(ns);
            }
            if (catalog.tableExists(name)) {
                catalog.dropTable(name, true);
            }

            Table table = catalog.createTable(name, schema, spec);

            GenericRecord record = GenericRecord.create(schema);
            record.setField("id", 1L);
            record.setField("data", "Hello, CdsIcebergS3Client!");

            String filepath = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
            OutputFile file = table.io().newOutputFile(filepath);

            GenericAppenderFactory appenderFactory = new GenericAppenderFactory(schema);
            EncryptedOutputFile encryptedFile = table.encryption().encrypt(file);
            try (DataWriter<Record> dataWriter = appenderFactory
                    .newDataWriter(encryptedFile, org.apache.iceberg.FileFormat.PARQUET, null)) {
                dataWriter.write(record);
                dataWriter.close();

                DataFile dataFile = dataWriter.toDataFile();
                table.newAppend().appendFile(dataFile).commit();
            }

            int rows = 0;
            try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
                for (Record readRecord : reader) {
                    rows++;
                    assertEquals(1L, readRecord.getField("id"));
                    assertEquals("Hello, CdsIcebergS3Client!", readRecord.getField("data"));
                }
            }
            assertEquals(1, rows);
        } finally {
            catalog.close();
        }
    }
}
