package io.github.has3a;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SmartS3ClientIntegrationTest {

    private static S3Client s3Client;
    private static final String BUCKET_NAME = "integration-test-bucket";
    private static final String OBJECT_KEY = "test-object.txt";
    private static final String MULTIPART_OBJECT_KEY = "integration-multipart-object.bin";
    private static final String CONTENT = "Hello, MinIO Integration Test from Bulkhead Proxy!";

    @BeforeAll
    public static void setup() {
        // MinIO default credentials
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        URI endpoint = URI.create("http://127.0.0.1:9000");

        s3Client = BulkheadS3ClientFactory.createSmartProxy(
                Collections.singletonList(endpoint),
                accessKey,
                secretKey,
                Region.US_EAST_1 
        );

        // Metadata Operation - target: metadataHttpClient
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
            System.out.println("Bucket created: " + BUCKET_NAME);
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
            System.out.println("Bucket already exists: " + BUCKET_NAME);
        }
    }

    @Test
    public void testEndToEndOperations() throws Exception {
        // 1. Put Object (Data operation -> dataHttpClient)
        System.out.println("Uploading object...");
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(OBJECT_KEY).build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(CONTENT));

        // 2. Head Object (Metadata operation -> metadataHttpClient)
        System.out.println("Fetching object metadata...");
        HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(OBJECT_KEY)
                .build());
        assertEquals(CONTENT.length(), headObjectResponse.contentLength());

        // 3. Get Object (Data operation -> dataHttpClient)
        System.out.println("Downloading object...");
        String downloadedContent = s3Client.getObject(GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(OBJECT_KEY)
                .build(), software.amazon.awssdk.core.sync.ResponseTransformer.toBytes())
                .asString(StandardCharsets.UTF_8);
        assertEquals(CONTENT, downloadedContent);

        // 4. List Objects (Metadata operation -> metadataHttpClient)
        System.out.println("Listing objects...");
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build());
        assertTrue(listResponse.contents().stream().anyMatch(o -> o.key().equals(OBJECT_KEY)));
        
        System.out.println("End to end integration test passed!");
    }

    /**
     * Multipart: createMultipartUpload → uploadPart (5 MiB + small last part per S3/MinIO rules) → complete → getObject.
     */
    @Test
    public void testMultipartUploadRoundTrip() {
        final int minPartBytes = 5 * 1024 * 1024;
        byte[] part1 = new byte[minPartBytes];
        Arrays.fill(part1, (byte) 'A');
        String part2 = "multipart-tail";

        CreateMultipartUploadResponse created = s3Client.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(MULTIPART_OBJECT_KEY)
                        .build());
        String uploadId = created.uploadId();
        assertNotNull(uploadId);

        try {
            System.out.println("Multipart: initiate uploadId=" + uploadId);

            UploadPartResponse up1 = s3Client.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(BUCKET_NAME)
                            .key(MULTIPART_OBJECT_KEY)
                            .uploadId(uploadId)
                            .partNumber(1)
                            .build(),
                    RequestBody.fromBytes(part1));

            UploadPartResponse up2 = s3Client.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(BUCKET_NAME)
                            .key(MULTIPART_OBJECT_KEY)
                            .uploadId(uploadId)
                            .partNumber(2)
                            .build(),
                    RequestBody.fromString(part2));

            CompletedPart cp1 = CompletedPart.builder().partNumber(1).eTag(up1.eTag()).build();
            CompletedPart cp2 = CompletedPart.builder().partNumber(2).eTag(up2.eTag()).build();

            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(MULTIPART_OBJECT_KEY)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(cp1, cp2).build())
                    .build());

            byte[] downloaded = s3Client.getObject(
                    GetObjectRequest.builder().bucket(BUCKET_NAME).key(MULTIPART_OBJECT_KEY).build(),
                    ResponseTransformer.toBytes())
                    .asByteArray();

            assertEquals(minPartBytes + part2.length(), downloaded.length);
            assertEquals((byte) 'A', downloaded[0]);
            assertEquals((byte) 'A', downloaded[minPartBytes - 1]);
            assertEquals(part2, new String(downloaded, minPartBytes, part2.length(), StandardCharsets.UTF_8));
            System.out.println("Multipart upload integration test passed!");
        } catch (RuntimeException e) {
            try {
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(MULTIPART_OBJECT_KEY)
                        .uploadId(uploadId)
                        .build());
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    @AfterAll
    public static void cleanup() {
        if (s3Client != null) {
            try {
                // Delete Object (Metadata operation)
                s3Client.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key(OBJECT_KEY).build());
                s3Client.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key(MULTIPART_OBJECT_KEY).build());
                // Delete Bucket (Metadata operation)
                s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET_NAME).build());
                System.out.println("Cleanup completed.");
            } catch (Exception e) {
                System.out.println("Cleanup failed or already cleaned.");
            }
        }
    }
}
