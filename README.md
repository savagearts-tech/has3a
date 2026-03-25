# has3a

Multi-endpoint **MinIO / S3-compatible** Java client: **bulkhead** metadata vs data HTTP pools, **round-robin** with optional node quarantine, and **multipart `uploadId` affinity** across `createMultipartUpload` → parts → complete/abort.

- **Java 17**, **Maven**, AWS SDK for Java v2 (`aws.java.sdk.version` in `pom.xml`).
- **Iceberg**: `CdsIcebergS3ClientFactory` implements `AwsClientFactory` for `S3FileIO`.

## Multipart and data-tier routing

Operations such as `uploadPart`, `listParts`, and `uploadPartCopy` use the **data** connection pool and honor **sticky routing** to the node that created the `uploadId`. Calling **`close()`** on the proxied client clears all sticky state and closes underlying clients.

## Maintainer: SDK upgrades

When upgrading the AWS SDK BOM, follow [docs/sdk-upgrade-audit.md](docs/sdk-upgrade-audit.md) to keep `DATA_OPERATIONS` and `uploadId` extraction aligned with `S3Client`.

## Build & test

```bash
./mvnw clean test
```

Integration tests expect MinIO at `http://127.0.0.1:9000` (see `SmartS3ClientIntegrationTest`).
