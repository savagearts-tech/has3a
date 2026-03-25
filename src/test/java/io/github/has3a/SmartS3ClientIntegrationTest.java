package io.github.has3a;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SmartS3ClientIntegrationTest {

    private static S3Client s3Client;
    private static final String BUCKET_NAME = "integration-test-bucket";
    private static final String OBJECT_KEY = "test-object.txt";
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

    @AfterAll
    public static void cleanup() {
        if (s3Client != null) {
            try {
                // Delete Object (Metadata operation)
                s3Client.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key(OBJECT_KEY).build());
                // Delete Bucket (Metadata operation)
                s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET_NAME).build());
                System.out.println("Cleanup completed.");
            } catch (Exception e) {
                System.out.println("Cleanup failed or already cleaned.");
            }
        }
    }
}
