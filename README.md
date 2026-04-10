# has3a

Multi-endpoint **MinIO / S3-compatible** Java client: **bulkhead** metadata vs data HTTP pools, **round-robin** with optional node quarantine, and **multipart `uploadId` affinity** across `createMultipartUpload` → parts → complete/abort.

- **Java 17**, **Maven**, AWS SDK for Java v2 (`aws.java.sdk.version` in `pom.xml`).
- **Iceberg**: `CdsIcebergS3ClientFactory` implements `AwsClientFactory` for `S3FileIO`. The MinIO integration test uses `JdbcCatalog` (H2) and `s3://` warehouse paths—no Hadoop `s3a://` / S3A client configuration. Tests depend on `hadoop-common` and `hadoop-mapreduce-client-core` only so Iceberg Parquet can load Hadoop types; object I/O still uses `S3FileIO`, not `hadoop-aws` / S3A.
- **S3FileIO staging**: use Iceberg `s3.staging-directory` on a data volume, or call `IcebergS3FileIOStaging.applyPreferredStagingDirectory(catalogProps)` (env `HAS3A_S3_STAGING_DIRECTORY` / property `has3a.s3.staging-directory`) so uploads do not fill `java.io.tmpdir` on the system disk.

## Multipart and data-tier routing

Operations such as `uploadPart`, `listParts`, and `uploadPartCopy` use the **data** connection pool and honor **sticky routing** to the node that created the `uploadId`. Calling **`close()`** on the proxied client clears all sticky state and closes underlying clients.

## Maintainer: SDK upgrades

When upgrading the AWS SDK BOM, follow [docs/sdk-upgrade-audit.md](docs/sdk-upgrade-audit.md) to keep `DATA_OPERATIONS` and `uploadId` extraction aligned with `S3Client`.

## Build & test

```bash
./mvnw clean test
```

Integration tests expect MinIO at `http://127.0.0.1:9000` (see `SmartS3ClientIntegrationTest`).
