package io.github.has3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class IcebergHadoopCatalogExampleTest {

    /**
     * Pushes {@link BulkheadClientConfig} into catalog properties consumed by
     * {@link CdsIcebergS3ClientFactory} (same keys as {@code parseConfig}).
     */
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
    public void testIcebergHadoopCatalogReadWrite() throws IOException {
        // Windows: avoid Hadoop startup errors (HADOOP_HOME / winutils)
        java.io.File dummyHadoop = new java.io.File(System.getProperty("java.io.tmpdir"), "hadoop-dummy");
        java.io.File binDir = new java.io.File(dummyHadoop, "bin");
        binDir.mkdirs();
        new java.io.File(binDir, "winutils.exe").createNewFile();
        System.setProperty("hadoop.home.dir", dummyHadoop.getAbsolutePath());

        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", "minioadmin");
        conf.set("fs.s3a.secret.key", "minioadmin");
        conf.set("fs.s3a.endpoint", "http://127.0.0.1:9000");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        // Avoid local disk staging (uses JNI on Windows); keeps test runnable without Hadoop native libs
        conf.set("fs.s3a.fast.upload.buffer", "bytebuffer");

        String warehousePath = "s3a://integration-test-bucket/warehouse";

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);

        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("client.factory", "io.github.has3a.CdsIcebergS3ClientFactory");
        properties.put("s3.endpoint.list", "http://127.0.0.1:9000");
        properties.put("s3.access-key-id", "minioadmin");
        properties.put("s3.secret-access-key", "minioadmin");

        // BulkheadClientConfig sample: programmatic tuning, then mirrored into Iceberg catalog props for CdsIcebergS3ClientFactory
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

        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(conf);
        catalog.initialize("hadoop_s3_catalog", properties);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();
        TableIdentifier name = TableIdentifier.of("default", "sample_table");

        if (catalog.tableExists(name)) {
            catalog.dropTable(name);
        }

        System.out.println("Creating table: " + name);
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
            System.out.println("Data successfully written to table!");
        }

        System.out.println("Reading data from table:");
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record readRecord : reader) {
                System.out.println("Read Record -> id: " + readRecord.getField("id") + ", data: " + readRecord.getField("data"));
            }
        }
    }
}
