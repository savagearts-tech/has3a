# AWS SDK upgrade audit (S3Client ↔ SmartS3ClientProxy)

When bumping `software.amazon.awssdk` BOM, reconcile **synchronous** `S3Client` APIs with this project:

1. **Data tier** — `SmartS3ClientProxy` field `DATA_OPERATIONS` (`SmartS3ClientProxy.java`).  
   - Must include any new method that transfers object bytes, streams body payload, or acts on an **in-progress multipart upload** (`uploadId`), e.g. `uploadPart`, `listParts`, `uploadPartCopy`.  
   - Typically **exclude** bucket-wide listings such as `listMultipartUploads` (metadata / control-plane style).

2. **Sticky `uploadId`** — `extractUploadId` in the same class.  
   - For each data-tier request type that carries `uploadId`, add an `instanceof` branch (or equivalent) so multipart affinity applies.

3. **Quick enumeration** (optional): from repo root with JDK on `PATH`:

   ```bash
   javap -public -classpath "$HOME/.m2/repository/software/amazon/awssdk/s3/<version>/s3-<version>.jar" \
     software.amazon.awssdk.services.s3.S3Client
   ```

4. Run `mvn test` including MinIO integration and Iceberg catalog tests.

See also: OpenSpec change `harden-s3-proxy-routing-and-lifecycle`.
