package io.github.has3a;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class SmartS3ClientProxy implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(SmartS3ClientProxy.class);

    private final S3Client[] metadataClients;
    private final S3Client[] dataClients;
    private final URI[]      endpoints;

    private final S3ClientMetrics metrics;

    // ── round-robin & quarantine ──────────────────────────────────────────────
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, Long> quarantinedNodes = new ConcurrentHashMap<>();
    private final long quarantineTtlMillis;

    // ── observable counters ───────────────────────────────────────────────────
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong failoverCount = new AtomicLong(0);
    private final AtomicLong errorCount    = new AtomicLong(0);
    /** How many times each node has been quarantined (indexed by node position). */
    private final AtomicLongArray quarantineCounts;

    // ── fast-path routing ─────────────────────────────────────────────────────
    // Fix #6: Added missing multipart + getObjectAsBytes operations to DATA tier
    private static final Set<String> DATA_OPERATIONS = Set.of(
            "putObject", "getObject", "uploadPart", "copyObject",
            "getObjectAsBytes",
            "createMultipartUpload", "completeMultipartUpload", "abortMultipartUpload"
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────────────

    private SmartS3ClientProxy(
            List<S3Client>  metadataClients,
            List<S3Client>  dataClients,
            List<URI>       endpoints,
            S3ClientMetrics metrics,
            long            quarantineTtlMillis) {

        this.metadataClients    = metadataClients.toArray(new S3Client[0]);
        this.dataClients        = dataClients.toArray(new S3Client[0]);
        this.endpoints          = endpoints.toArray(new URI[0]);
        this.metrics            = metrics;
        this.quarantineTtlMillis = quarantineTtlMillis;
        this.quarantineCounts   = new AtomicLongArray(endpoints.size());

        if (metrics == S3ClientMetrics.NO_OP) {
            log.warn("SmartS3ClientProxy created with no-op metrics �?" +
                     "pass a S3ClientMetrics implementation to enable observability");
        }
    }

    public static S3Client create(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI>      endpoints) {
        return create(metadataClients, dataClients, endpoints, S3ClientMetrics.NO_OP);
    }

    public static S3Client create(
            List<S3Client>  metadataClients,
            List<S3Client>  dataClients,
            List<URI>       endpoints,
            S3ClientMetrics metrics) {
        return create(metadataClients, dataClients, endpoints, metrics, 30_000L);
    }

    public static S3Client create(
            List<S3Client>  metadataClients,
            List<S3Client>  dataClients,
            List<URI>       endpoints,
            S3ClientMetrics metrics,
            long            quarantineTtlMillis) {

        SmartS3ClientProxy handler =
                new SmartS3ClientProxy(metadataClients, dataClients, endpoints, metrics, quarantineTtlMillis);
        return (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                handler);
    }

    /** Convenience accessor �?unwraps the handler from a proxy created by this class. */
    public static SmartS3ClientProxy handlerOf(S3Client proxy) {
        return (SmartS3ClientProxy) Proxy.getInvocationHandler(proxy);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // InvocationHandler
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        // Delegate Object methods to the handler itself
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }

        // Fix #3: Intercept close() and propagate to ALL underlying clients
        if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
            closeAll();
            return null;
        }

        boolean isDataOperation = DATA_OPERATIONS.contains(method.getName());
        S3Client[] targetClients = isDataOperation ? dataClients : metadataClients;
        S3ClientMetrics.Tier tier = isDataOperation
                ? S3ClientMetrics.Tier.DATA
                : S3ClientMetrics.Tier.METADATA;
        int maxNodes = targetClients.length;
        Throwable lastException = null;

        totalRequests.incrementAndGet();

        for (int attempt = 0; attempt < maxNodes; attempt++) {
            int nodeIndex = getHealthyNodeIndex(maxNodes);
            S3Client activeClient = targetClients[nodeIndex];
            long startNs = System.nanoTime();

            try {
                Object result = method.invoke(activeClient, args);
                long durationNs = System.nanoTime() - startNs;
                metrics.record(new S3ClientMetrics.CallRecord(
                        method.getName(), tier, nodeIndex, durationNs,
                        S3ClientMetrics.Outcome.SUCCESS));
                return result;

            } catch (InvocationTargetException e) {
                long durationNs = System.nanoTime() - startNs;
                Throwable cause = e.getCause();

                if (isNetworkFailure(cause)) {
                    quarantineNode(nodeIndex);
                    failoverCount.incrementAndGet();
                    metrics.record(new S3ClientMetrics.CallRecord(
                            method.getName(), tier, nodeIndex, durationNs,
                            S3ClientMetrics.Outcome.FAILOVER));
                    lastException = cause;
                    continue; // failover to next node
                }

                // Business exception (4xx, checksum failure, etc.) �?propagate immediately
                metrics.record(new S3ClientMetrics.CallRecord(
                        method.getName(), tier, nodeIndex, durationNs,
                        S3ClientMetrics.Outcome.ERROR));
                throw cause;
            }
        }

        // All nodes exhausted
        errorCount.incrementAndGet();
        Throwable toThrow = lastException != null
                ? lastException
                : new RuntimeException("All MinIO nodes failed.");
        log.error("operation={} all nodes failed �?last exception: {}",
                method.getName(), toThrow.getMessage());
        throw toThrow;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Routing helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Fix #1: Consume exactly ONE counter token per invocation, then scan linearly.
     * <p>
     * Previous implementation called {@code getAndIncrement()} inside the scan loop,
     * causing token leakage that skewed the round-robin distribution and could result
     * in two concurrent threads receiving the same nodeIndex.
     */
    private int getHealthyNodeIndex(int maxNodes) {
        int start = (roundRobinCounter.getAndIncrement() & Integer.MAX_VALUE) % maxNodes;
        for (int i = 0; i < maxNodes; i++) {
            int candidate = (start + i) % maxNodes;
            Long expiry = quarantinedNodes.get(candidate);
            if (expiry == null || System.currentTimeMillis() > expiry) {
                if (expiry != null) {
                    quarantinedNodes.remove(candidate);
                    log.info("action=quarantine_lift node={} endpoint={}", candidate, endpoints[candidate]);
                }
                return candidate;
            }
        }
        // All nodes quarantined �?degrade to start, consuming no extra counter tokens
        return start;
    }

    /**
     * Fix #5: Use {@code putIfAbsent} to avoid duplicate quarantine events.
     * <p>
     * If the node is already in quarantine, silently refresh the TTL without
     * emitting another WARN log line or incrementing the quarantine counter.
     */
    private void quarantineNode(int nodeIndex) {
        long expiry = System.currentTimeMillis() + quarantineTtlMillis;
        Long previous = quarantinedNodes.putIfAbsent(nodeIndex, expiry);
        if (previous == null) {
            // First quarantine in this TTL window
            quarantineCounts.incrementAndGet(nodeIndex);
            log.warn("action=quarantine_start node={} endpoint={} expiresAt={}",
                    nodeIndex, endpoints[nodeIndex], expiry);
        } else {
            // Already quarantined �?silently refresh TTL to extend isolation period
            quarantinedNodes.put(nodeIndex, expiry);
        }
    }

    /**
     * Fix #2: Only treat genuine network-layer failures as quarantine triggers.
     * <p>
     * The former implementation included plain {@code RetryableException}, which
     * covers HTTP 429 (throttling) and checksum failures �?neither indicates a node
     * is unreachable and neither warrants quarantining the node.
     */
    private boolean isNetworkFailure(Throwable t) {
        if (t instanceof IOException) return true;
        if (t instanceof ApiCallAttemptTimeoutException) return true;
        // Accept SdkClientException only when its root cause is an IOException
        // (e.g., RetryableException wrapping a SocketException from a TCP reset)
        if (t instanceof SdkClientException sce) {
            Throwable cause = sce.getCause();
            return cause instanceof IOException;
        }
        return false;
    }

    // Fix #3: Close ALL underlying S3Clients to prevent ApacheHttpClient pool leaks
    private void closeAll() {
        for (S3Client c : metadataClients) closeQuietly(c);
        for (S3Client c : dataClients)     closeQuietly(c);
    }

    private void closeQuietly(S3Client c) {
        try {
            c.close();
        } catch (Exception e) {
            log.warn("Failed to close S3Client: {}", e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Observable stats
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns an immutable point-in-time snapshot of proxy counters.
     * Safe to call from any thread; reads are non-blocking.
     */
    public ProxyStats getStats() {
        int nodeCount = endpoints.length;
        List<ProxyStats.NodeStats> nodeStatsList = new ArrayList<>(nodeCount);
        long now = System.currentTimeMillis();
        for (int i = 0; i < nodeCount; i++) {
            Long expiry = quarantinedNodes.get(i);
            boolean quarantined = expiry != null && now <= expiry;
            nodeStatsList.add(new ProxyStats.NodeStats(i, quarantineCounts.get(i), quarantined));
        }
        return new ProxyStats(
                totalRequests.get(),
                failoverCount.get(),
                errorCount.get(),
                List.copyOf(nodeStatsList));
    }
}
