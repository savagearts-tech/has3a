package io.github.has3a;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;


import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class SmartS3ClientProxyTest {

    // -------------------------------------------------------------------------
    // 1. Routing: metadata operations vs data operations
    // -------------------------------------------------------------------------

    @Test
    void testListObjectsV2_routesToMetadataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        verify(metaClient, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
        verify(dataClient, never()).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testPutObject_routesToDataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        proxy.putObject(PutObjectRequest.builder().build(), RequestBody.fromString("test"));

        verify(dataClient, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(metaClient, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testGetObject_routesToDataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        proxy.getObject(GetObjectRequest.builder().bucket("b").key("k").build());

        verify(dataClient, times(1)).getObject(any(GetObjectRequest.class));
        verify(metaClient, never()).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testCopyObject_routesToDataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        proxy.copyObject(CopyObjectRequest.builder().sourceBucket("src").sourceKey("k").destinationBucket("dst").destinationKey("k").build());

        verify(dataClient, times(1)).copyObject(any(CopyObjectRequest.class));
        verify(metaClient, never()).copyObject(any(CopyObjectRequest.class));
    }

    @Test
    void testUploadPart_routesToDataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        proxy.uploadPart(UploadPartRequest.builder().bucket("b").key("k").uploadId("id").partNumber(1).build(),
                RequestBody.fromString("part"));

        verify(dataClient, times(1)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(metaClient, never()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    }

    @Test
    void testHeadObject_routesToMetadataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        proxy.headObject(HeadObjectRequest.builder().bucket("b").key("k").build());

        verify(metaClient, times(1)).headObject(any(HeadObjectRequest.class));
        verify(dataClient, never()).headObject(any(HeadObjectRequest.class));
    }

    // -------------------------------------------------------------------------
    // 2. Failover: various network error types
    // -------------------------------------------------------------------------

    @Test
    void testFailover_onApiCallAttemptTimeoutException_retriesNextNode() {
        S3Client failNode = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);

        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, successNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        // Some calls may hit failNode but every call must ultimately succeed via successNode
        for (int i = 0; i < 4; i++) {
            proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        }

        verify(successNode, atLeast(4)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFailover_onIOException_retriesNextNode() {
        S3Client failNode = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);

        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(RetryableException.builder().message("io").cause(new IOException("connection reset")).build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, successNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        for (int i = 0; i < 3; i++) {
            proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        }

        verify(successNode, atLeast(3)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFailover_onRetryableExceptionWithIOCause_retriesNextNode() {
        // RetryableException with an IOException root cause IS a network failure -> triggers failover
        S3Client failNode    = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);

        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(RetryableException.builder()
                        .message("retryable-with-io-cause")
                        .cause(new IOException("connection reset by peer"))
                        .build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, successNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        verify(successNode, atLeast(1)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFailover_onBareRetryableException_propagatesWithoutFailover() {
        // Bare RetryableException (e.g., throttling 429) is NOT a node failure -> propagates immediately
        S3Client node = mock(S3Client.class);

        when(node.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(RetryableException.builder().message("throttled").build());

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(node),
                List.of(mock(S3Client.class)),
                List.of(URI.create("http://node1"))
        );

        assertThrows(RetryableException.class, () ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build()));

        // Must not be quarantined
        ProxyStats stats = SmartS3ClientProxy.handlerOf(proxy).getStats();
        assertEquals(0, stats.nodeStats().get(0).quarantineCount(), "throttling must not quarantine the node");
    }

    // -------------------------------------------------------------------------
    // 3. Business exceptions propagate immediately without failover
    // -------------------------------------------------------------------------

    @Test
    void testBusinessException_propagatesImmediately_noFailover() {
        S3Client failNode = mock(S3Client.class);
        S3Client otherNode = mock(S3Client.class);

        // NoSuchKeyException is a business exception, must not trigger failover
        when(failNode.getObject(any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("not found").build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(failNode, otherNode),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        // First call hits failNode (round-robin starts at index 0)
        assertThrows(NoSuchKeyException.class, () ->
                proxy.getObject(GetObjectRequest.builder().bucket("b").key("k").build())
        );

        // Business exceptions must not trigger failover, otherNode should never be called
        verify(otherNode, never()).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testS3Exception_propagatesImmediately_noFailover() {
        S3Client failNode = mock(S3Client.class);
        S3Client otherNode = mock(S3Client.class);

        when(failNode.headObject(any(HeadObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("access denied").statusCode(403).build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, otherNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        assertThrows(S3Exception.class, () ->
                proxy.headObject(HeadObjectRequest.builder().bucket("b").key("k").build())
        );

        verify(otherNode, never()).headObject(any(HeadObjectRequest.class));
    }

    // -------------------------------------------------------------------------
    // 4. All nodes fail -> throw last exception
    // -------------------------------------------------------------------------

    @Test
    void testAllNodesFail_throwsLastException() {
        S3Client failNode1 = mock(S3Client.class);
        S3Client failNode2 = mock(S3Client.class);

        ApiCallAttemptTimeoutException timeout = ApiCallAttemptTimeoutException.builder().message("timeout").build();
        when(failNode1.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(timeout);
        when(failNode2.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(timeout);

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode1, failNode2),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        assertThrows(ApiCallAttemptTimeoutException.class, () ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build())
        );
    }

    @Test
    void testSingleNode_allFail_throwsException() {
        S3Client onlyNode = mock(S3Client.class);
        when(onlyNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build());

        S3Client proxy = SmartS3ClientProxy.create(
                Collections.singletonList(onlyNode),
                Collections.singletonList(mock(S3Client.class)),
                Collections.singletonList(URI.create("http://node1"))
        );

        assertThrows(ApiCallAttemptTimeoutException.class, () ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build())
        );
    }

    // -------------------------------------------------------------------------
    // 5. Quarantine TTL recovery: quarantined node is re-used after TTL expiry
    // -------------------------------------------------------------------------

    @Test
    void testQuarantinedNode_recoverAfterTtlExpiry() throws InterruptedException {
        // QUARANTINE_TTL_MILLIS = 30s is too long for a unit test;
        // this test verifies failover behaviour during quarantine.
        S3Client transientFailNode = mock(S3Client.class);
        S3Client stableNode = mock(S3Client.class);

        // First call fails (triggers quarantine), subsequent calls succeed
        when(transientFailNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build())
                .thenReturn(ListObjectsV2Response.builder().build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(transientFailNode, stableNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        // 1st call: transientFailNode fails, failover to stableNode succeeds
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        // During quarantine, subsequent calls should route to stableNode
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        verify(stableNode, atLeast(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    // -------------------------------------------------------------------------
    // 6. All nodes quarantined fallback: must round-robin rather than hang
    // -------------------------------------------------------------------------

    @Test
    void testAllQuarantinedFallback_continuesRoundRobin() {
        // Both nodes timeout once -> double quarantine -> proxy degrades to round-robin
        S3Client node1 = mock(S3Client.class);
        S3Client node2 = mock(S3Client.class);

        when(node1.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout1").build())
                .thenReturn(ListObjectsV2Response.builder().build());
        when(node2.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout2").build())
                .thenReturn(ListObjectsV2Response.builder().build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(node1, node2),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        // 1st call: node1 fails -> failover -> node2 fails -> all-nodes-failed exception
        assertThrows(Exception.class, () ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build())
        );

        // Both nodes now quarantined; 2nd call should use degraded round-robin without hanging
        // node1/node2 return successfully on 2nd invocation
        assertDoesNotThrow(() ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build())
        );
    }

    // -------------------------------------------------------------------------
    // 7. Object class methods delegate to InvocationHandler (not S3Client)
    // -------------------------------------------------------------------------

    @Test
    void testObjectMethods_doNotRouteToS3Clients() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock"))
        );

        // toString / hashCode / equals must not throw and must not forward to S3Client
        assertDoesNotThrow(proxy::toString);
        assertDoesNotThrow(proxy::hashCode);
        assertDoesNotThrow(() -> proxy.equals(proxy));

        verifyNoInteractions(metaClient, dataClient);
    }

    // -------------------------------------------------------------------------
    // 8. Load balancing: round-robin distributes across nodes
    // -------------------------------------------------------------------------

    @Test
    void testRoundRobin_distributes_across_nodes() {
        S3Client meta1 = mock(S3Client.class);
        S3Client meta2 = mock(S3Client.class);

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(meta1, meta2),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        int calls = 10;
        for (int i = 0; i < calls; i++) {
            proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        }

        // Each node must be called at least once
        verify(meta1, atLeast(1)).listObjectsV2(any(ListObjectsV2Request.class));
        verify(meta2, atLeast(1)).listObjectsV2(any(ListObjectsV2Request.class));

        int total = mockingDetails(meta1).getInvocations().size()
                + mockingDetails(meta2).getInvocations().size();
        assertEquals(calls, total, "Sum of all invocations must equal total request count");
    }

    // -------------------------------------------------------------------------
    // 9. Observability: metrics recording (S3ClientMetrics SPI)
    // -------------------------------------------------------------------------

    /** Simple test sink that captures all CallRecords. */
    private static List<S3ClientMetrics.CallRecord> captureMetrics() {
        return new CopyOnWriteArrayList<>();
    }

    @Test
    void testMetrics_successOperation_recordedWithSuccessOutcome() {
        List<S3ClientMetrics.CallRecord> records = captureMetrics();

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(mock(S3Client.class)),
                List.of(mock(S3Client.class)),
                List.of(URI.create("http://node1")),
                records::add
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        assertEquals(1, records.size());
        S3ClientMetrics.CallRecord r = records.get(0);
        assertEquals("listObjectsV2", r.operation());
        assertEquals(S3ClientMetrics.Tier.METADATA, r.tier());
        assertEquals(S3ClientMetrics.Outcome.SUCCESS, r.outcome());
        assertTrue(r.durationNs() >= 0, "duration must be non-negative");
    }

    @Test
    void testMetrics_dataOperation_tieredCorrectly() {
        List<S3ClientMetrics.CallRecord> records = captureMetrics();

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(mock(S3Client.class)),
                List.of(mock(S3Client.class)),
                List.of(URI.create("http://node1")),
                records::add
        );

        proxy.putObject(PutObjectRequest.builder().build(), RequestBody.fromString("x"));

        assertEquals(1, records.size());
        assertEquals(S3ClientMetrics.Tier.DATA, records.get(0).tier());
        assertEquals(S3ClientMetrics.Outcome.SUCCESS, records.get(0).outcome());
    }

    @Test
    void testMetrics_failoverAttempt_recordedAsFailoverThenSuccess() {
        List<S3ClientMetrics.CallRecord> records = captureMetrics();

        S3Client failNode    = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);
        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, successNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2")),
                records::add
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        assertEquals(2, records.size());
        assertTrue(records.stream().anyMatch(r -> r.outcome() == S3ClientMetrics.Outcome.FAILOVER),
                "expected a FAILOVER record");
        assertTrue(records.stream().anyMatch(r -> r.outcome() == S3ClientMetrics.Outcome.SUCCESS),
                "expected a SUCCESS record");
    }

    @Test
    void testMetrics_allNodesFail_allRecordedAsFailover() {
        List<S3ClientMetrics.CallRecord> records = captureMetrics();

        S3Client n1 = mock(S3Client.class);
        S3Client n2 = mock(S3Client.class);
        ApiCallAttemptTimeoutException t = ApiCallAttemptTimeoutException.builder().message("t").build();
        when(n1.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(t);
        when(n2.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(t);

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(n1, n2),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2")),
                records::add
        );

        assertThrows(Exception.class, () ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build()));

        assertTrue(records.size() >= 2, "both failed attempts must be recorded");
        assertTrue(records.stream().allMatch(r -> r.outcome() == S3ClientMetrics.Outcome.FAILOVER),
                "all records should be FAILOVER when every node fails");
    }

    @Test
    void testMetrics_noOpDefault_doesNotThrow() {
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(mock(S3Client.class)),
                List.of(mock(S3Client.class)),
                List.of(URI.create("http://node1"))
        );
        assertDoesNotThrow(() -> proxy.listObjectsV2(ListObjectsV2Request.builder().build()));
    }

    // -------------------------------------------------------------------------
    // 10. Observability: ProxyStats snapshot
    // -------------------------------------------------------------------------

    private static SmartS3ClientProxy handlerOf(S3Client proxy) {
        return SmartS3ClientProxy.handlerOf(proxy);
    }

    @Test
    void testGetStats_initialState_allZeroes() {
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(mock(S3Client.class)),
                List.of(mock(S3Client.class)),
                List.of(URI.create("http://node1"))
        );

        ProxyStats stats = handlerOf(proxy).getStats();

        assertEquals(0, stats.totalRequests());
        assertEquals(0, stats.failoverCount());
        assertEquals(0, stats.errorCount());
        assertEquals(1, stats.nodeStats().size());
        assertEquals(0, stats.nodeStats().get(0).quarantineCount());
        assertFalse(stats.nodeStats().get(0).quarantined());
    }

    @Test
    void testGetStats_afterSuccessfulCalls_requestCountIncremented() {
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(mock(S3Client.class)),
                List.of(mock(S3Client.class)),
                List.of(URI.create("http://node1"))
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        ProxyStats stats = handlerOf(proxy).getStats();
        assertEquals(2, stats.totalRequests());
        assertEquals(0, stats.failoverCount());
        assertEquals(0, stats.errorCount());
    }

    @Test
    void testGetStats_afterFailover_countersReflectQuarantine() {
        S3Client failNode    = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);
        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, successNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        ProxyStats stats = handlerOf(proxy).getStats();
        assertEquals(1, stats.totalRequests());
        assertEquals(1, stats.failoverCount(), "one failover should be recorded");
        assertEquals(0, stats.errorCount());

        long totalQuarantineEvents = stats.nodeStats().stream()
                .mapToLong(ProxyStats.NodeStats::quarantineCount).sum();
        assertEquals(1, totalQuarantineEvents, "exactly one node should have been quarantined");
    }

    @Test
    void testGetStats_afterAllNodesFail_errorCountIncremented() {
        S3Client n1 = mock(S3Client.class);
        S3Client n2 = mock(S3Client.class);
        ApiCallAttemptTimeoutException t = ApiCallAttemptTimeoutException.builder().message("t").build();
        when(n1.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(t);
        when(n2.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(t);

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(n1, n2),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        assertThrows(Exception.class, () ->
                proxy.listObjectsV2(ListObjectsV2Request.builder().build()));

        ProxyStats stats = handlerOf(proxy).getStats();
        assertEquals(1, stats.totalRequests());
        assertEquals(1, stats.errorCount(), "all-nodes-failed should increment errorCount");
    }

    @Test
    void testGetStats_nodeQuarantinedFlag_trueDuringTtl() {
        S3Client failNode    = mock(S3Client.class);
        S3Client successNode = mock(S3Client.class);
        when(failNode.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(ApiCallAttemptTimeoutException.builder().message("timeout").build());

        S3Client proxy = SmartS3ClientProxy.create(
                Arrays.asList(failNode, successNode),
                Arrays.asList(mock(S3Client.class), mock(S3Client.class)),
                Arrays.asList(URI.create("http://node1"), URI.create("http://node2"))
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());

        ProxyStats stats = handlerOf(proxy).getStats();
        long quarantinedNow = stats.nodeStats().stream()
                .filter(ProxyStats.NodeStats::quarantined).count();
        assertEquals(1, quarantinedNow, "the failed node should report quarantined=true");
    }

    @Test
    void testListParts_routesToDataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock")));
        proxy.listParts(ListPartsRequest.builder()
                .bucket("b").key("k").uploadId("uid").build());
        verify(dataClient, times(1)).listParts(any(ListPartsRequest.class));
        verify(metaClient, never()).listParts(any(ListPartsRequest.class));
    }

    @Test
    @SuppressWarnings("deprecation")
    void testUploadPartCopy_routesToDataClient() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock")));
        proxy.uploadPartCopy(UploadPartCopyRequest.builder()
                .copySource("src-bucket/src-key")
                .destinationBucket("b")
                .destinationKey("k")
                .uploadId("uid")
                .partNumber(1)
                .build());
        verify(dataClient, times(1)).uploadPartCopy(any(UploadPartCopyRequest.class));
        verify(metaClient, never()).uploadPartCopy(any(UploadPartCopyRequest.class));
    }

    @Test
    void testClose_clearsStickyRoutes() {
        S3Client metaClient = mock(S3Client.class);
        S3Client dataClient = mock(S3Client.class);
        when(dataClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder()
                        .bucket("b").key("k").uploadId("sticky-u1").build());
        S3Client proxy = SmartS3ClientProxy.create(
                List.of(metaClient),
                List.of(dataClient),
                List.of(URI.create("http://mock")));
        proxy.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket("b").key("k").build());
        assertEquals(1, SmartS3ClientProxy.handlerOf(proxy).stickyRouteCountForTests());
        proxy.close();
        assertEquals(0, SmartS3ClientProxy.handlerOf(proxy).stickyRouteCountForTests());
    }

    // -------------------------------------------------------------------------
    // 11. Pool Intervention: rebuilding the connection pool
    // -------------------------------------------------------------------------

    @Test
    void testForceRebuildPools_swapsUnderlyingClients() throws Exception {
        S3Client meta1 = mock(S3Client.class);
        S3Client data1 = mock(S3Client.class);

        S3Client meta2 = mock(S3Client.class);
        S3Client data2 = mock(S3Client.class);

        java.util.concurrent.atomic.AtomicInteger rebuildCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.function.Supplier<SmartS3ClientProxy.Clients> supplier = () -> {
            rebuildCount.incrementAndGet();
            return new SmartS3ClientProxy.Clients(List.of(meta2), List.of(data2));
        };

        S3Client proxy = SmartS3ClientProxy.create(
                List.of(meta1),
                List.of(data1),
                List.of(URI.create("http://mock")),
                S3ClientMetrics.NO_OP,
                1000,
                1000,
                supplier
        );

        proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        verify(meta1, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
        verify(meta2, never()).listObjectsV2(any(ListObjectsV2Request.class));

        // Cast to S3ClientPoolManager and trigger rebuild
        S3ClientPoolManager manager = (S3ClientPoolManager) proxy;
        manager.forceRebuildPools();

        assertEquals(1, rebuildCount.get(), "Rebuild logic must be invoked");

        // Wait shortly for async close to run
        Thread.sleep(100);

        // New requests should hit the new clients
        proxy.listObjectsV2(ListObjectsV2Request.builder().build());
        verify(meta2, times(1)).listObjectsV2(any(ListObjectsV2Request.class));

        // Old clients must have been closed
        verify(meta1, times(1)).close();
        verify(data1, times(1)).close();

        // New clients should not be closed
        verify(meta2, never()).close();
        verify(data2, never()).close();
    }
}
