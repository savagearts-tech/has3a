package io.github.has3a;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;

public class SmartS3ClientProxy implements InvocationHandler, S3ClientPoolManager, S3ClientPoolManagerMBean {

    public static class Clients {
        public final List<S3Client> metadataClients;
        public final List<S3Client> dataClients;

        public Clients(List<S3Client> metadataClients, List<S3Client> dataClients) {
            this.metadataClients = metadataClients;
            this.dataClients = dataClients;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SmartS3ClientProxy.class);

    private volatile S3Client[] metadataClients;
    private volatile S3Client[] dataClients;
    private final URI[] endpoints;

    private final S3ClientMetrics metrics;
    private final Supplier<Clients> clientSupplier;

    // ── round-robin & quarantine ──────────────────────────────────────────────
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, Long> quarantinedNodes = new ConcurrentHashMap<>();
    private final long quarantineTtlMillis;

    // ── multipart stickiness ──────────────────────────────────────────────────
    private static class StickyRoute {
        final int nodeIndex;
        volatile long expiry;

        StickyRoute(int nodeIndex, long expiry) {
            this.nodeIndex = nodeIndex;
            this.expiry = expiry;
        }
    }

    private final ConcurrentHashMap<String, StickyRoute> stickyRoutes = new ConcurrentHashMap<>();
    private final long multipartRouteIdleTtlMillis;

    // ── observable counters ───────────────────────────────────────────────────
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong failoverCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    /** How many times each node has been quarantined (indexed by node position). */
    private final AtomicLongArray quarantineCounts;

    // ── mbean state ─────────────────────────────────────────────────────────
    private final AtomicLong poolRebuildCount = new AtomicLong(0);
    private volatile String lastRebuildTime = "N/A";

    // ── fast-path routing ─────────────────────────────────────────────────────
    /**
     * Data-tier operations (bulkhead data pool). Keep in sync with {@code S3Client}
     * when upgrading
     * the AWS SDK — see {@code docs/sdk-upgrade-audit.md}.
     */
    private static final Set<String> DATA_OPERATIONS = Set.of(
            "putObject", "getObject", "uploadPart", "copyObject",
            "getObjectAsBytes",
            "createMultipartUpload", "completeMultipartUpload", "abortMultipartUpload",
            "listParts", "uploadPartCopy");

    // ─────────────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────────────

    private SmartS3ClientProxy(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI> endpoints,
            S3ClientMetrics metrics,
            long quarantineTtlMillis,
            long multipartRouteIdleTtlMillis,
            Supplier<Clients> clientSupplier) {

        this.metadataClients = metadataClients.toArray(new S3Client[0]);
        this.dataClients = dataClients.toArray(new S3Client[0]);
        this.endpoints = endpoints.toArray(new URI[0]);
        this.metrics = metrics;
        this.quarantineTtlMillis = quarantineTtlMillis;
        this.multipartRouteIdleTtlMillis = multipartRouteIdleTtlMillis;
        this.quarantineCounts = new AtomicLongArray(endpoints.size());
        this.clientSupplier = clientSupplier;

        if (metrics == S3ClientMetrics.NO_OP) {
            log.warn(
                    "SmartS3ClientProxy created with no-op metrics - pass a S3ClientMetrics implementation to enable observability");
        }

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            String id = Integer.toHexString(System.identityHashCode(this));
            ObjectName name = new ObjectName("io.github.has3a:type=S3ClientPoolManager,name=SmartS3ClientProxy-" + id);
            if (!mbs.isRegistered(name)) {
                mbs.registerMBean(new StandardMBean(this, S3ClientPoolManagerMBean.class), name);
            }
        } catch (Exception e) {
            log.warn("Failed to register JMX MBean for S3ClientPoolManager: {}", e.getMessage());
        }
    }

    public static S3Client create(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI> endpoints) {
        return create(metadataClients, dataClients, endpoints, S3ClientMetrics.NO_OP);
    }

    public static S3Client create(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI> endpoints,
            S3ClientMetrics metrics) {
        return create(metadataClients, dataClients, endpoints, metrics,
                BulkheadClientConfig.DEFAULT_QUARANTINE_TTL_MILLIS,
                BulkheadClientConfig.DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS);
    }

    public static S3Client create(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI> endpoints,
            S3ClientMetrics metrics,
            long quarantineTtlMillis) {
        return create(metadataClients, dataClients, endpoints, metrics, quarantineTtlMillis,
                BulkheadClientConfig.DEFAULT_MULTIPART_ROUTE_IDLE_TTL_MILLIS);
    }

    public static S3Client create(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI> endpoints,
            S3ClientMetrics metrics,
            long quarantineTtlMillis,
            long multipartRouteIdleTtlMillis) {
        return create(metadataClients, dataClients, endpoints, metrics, quarantineTtlMillis,
                multipartRouteIdleTtlMillis, null);
    }

    public static S3Client create(
            List<S3Client> metadataClients,
            List<S3Client> dataClients,
            List<URI> endpoints,
            S3ClientMetrics metrics,
            long quarantineTtlMillis,
            long multipartRouteIdleTtlMillis,
            Supplier<Clients> clientSupplier) {

        SmartS3ClientProxy handler = new SmartS3ClientProxy(metadataClients, dataClients, endpoints, metrics,
                quarantineTtlMillis, multipartRouteIdleTtlMillis, clientSupplier);
        return (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[] { S3Client.class, S3ClientPoolManager.class },
                handler);
    }

    /**
     * Convenience accessor - unwraps the handler from a proxy created by this
     * class.
     */
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

        // Delegate PoolManager methods
        if (method.getDeclaringClass() == S3ClientPoolManager.class
                || method.getDeclaringClass() == S3ClientPoolManagerMBean.class) {
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

        String uploadId = extractUploadId(args);
        int stickyNodeIndex = -1;
        if (uploadId != null) {
            StickyRoute route = stickyRoutes.get(uploadId);
            if (route != null && System.currentTimeMillis() <= route.expiry) {
                stickyNodeIndex = route.nodeIndex;
                route.expiry = System.currentTimeMillis() + multipartRouteIdleTtlMillis; // lazy TTL refresh
            } else if (route != null) {
                stickyRoutes.remove(uploadId);
            }
        }

        int maxAttempts = stickyNodeIndex >= 0 ? 1 : maxNodes;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            int nodeIndex = stickyNodeIndex >= 0 ? stickyNodeIndex : getHealthyNodeIndex(maxNodes);
            S3Client activeClient = targetClients[nodeIndex];
            long startNs = System.nanoTime();

            try {
                Object result = method.invoke(activeClient, args);
                long durationNs = System.nanoTime() - startNs;

                // Track stickiness
                if (result instanceof CreateMultipartUploadResponse) {
                    CreateMultipartUploadResponse resp = (CreateMultipartUploadResponse) result;
                    String uid = resp.uploadId();
                    if (uid != null && !uid.isEmpty()) {
                        stickyRoutes.put(uid,
                                new StickyRoute(nodeIndex, System.currentTimeMillis() + multipartRouteIdleTtlMillis));
                    }
                } else if (result instanceof CompleteMultipartUploadResponse ||
                        result instanceof AbortMultipartUploadResponse) {
                    if (uploadId != null) {
                        stickyRoutes.remove(uploadId);
                    }
                }

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

                // Business exception (4xx, checksum failure, etc.) - propagate immediately
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
        log.error("operation={} all nodes failed - last exception: {}",
                method.getName(), toThrow.getMessage());
        throw toThrow;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Routing helpers
    // ─────────────────────────────────────────────────────────────────────────

    private String extractUploadId(Object[] args) {
        if (args == null || args.length == 0)
            return null;
        Object req = args[0];
        if (req instanceof UploadPartRequest)
            return ((UploadPartRequest) req).uploadId();
        if (req instanceof CompleteMultipartUploadRequest)
            return ((CompleteMultipartUploadRequest) req).uploadId();
        if (req instanceof AbortMultipartUploadRequest)
            return ((AbortMultipartUploadRequest) req).uploadId();
        if (req instanceof ListPartsRequest)
            return ((ListPartsRequest) req).uploadId();
        if (req instanceof UploadPartCopyRequest)
            return ((UploadPartCopyRequest) req).uploadId();
        return null;
    }

    /**
     * Fix #1: Consume exactly ONE counter token per invocation, then scan linearly.
     * <p>
     * Previous implementation called {@code getAndIncrement()} inside the scan
     * loop,
     * causing token leakage that skewed the round-robin distribution and could
     * result
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
        // All nodes quarantined - degrade to start, consuming no extra counter tokens
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
            // Already quarantined - silently refresh TTL to extend isolation period
            quarantinedNodes.put(nodeIndex, expiry);
        }
    }

    /**
     * Fix #2: Only treat genuine network-layer failures as quarantine triggers.
     * <p>
     * The former implementation included plain {@code RetryableException}, which
     * covers HTTP 429 (throttling) and checksum failures - neither indicates a node
     * is unreachable and neither warrants quarantining the node.
     */
    private boolean isNetworkFailure(Throwable t) {
        if (t instanceof IOException)
            return true;
        if (t instanceof ApiCallAttemptTimeoutException)
            return true;
        
        // AWS SDK often deeply nests exceptions (e.g., SdkClientException -> RetryableException -> ConnectException).
        // Safely traverse the entire cause chain to find any underlying network failure.
        Throwable cause = t.getCause();
        while (cause != null) {
            if (cause instanceof IOException) {
                return true;
            }
            if (cause == cause.getCause()) {
                break; // Prevent infinite loops in malformed exception chains
            }
            cause = cause.getCause();
        }
        return false;
    }

    // Fix #3: Close ALL underlying S3Clients to prevent ApacheHttpClient pool leaks
    private void closeAll() {
        stickyRoutes.clear();
        for (S3Client c : metadataClients)
            closeQuietly(c);
        for (S3Client c : dataClients)
            closeQuietly(c);
    }

    /**
     * Test hook: sticky multipart route count (always 0 after {@code close()}).
     */
    int stickyRouteCountForTests() {
        return stickyRoutes.size();
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

    @Override
    public void forceRebuildPools() {
        if (clientSupplier == null) {
            log.warn("Cannot force rebuild pools: no client supplier provided");
            return;
        }
        log.info("Force rebuilding all S3connection pools via intervention...");
        Clients newClients;
        try {
            newClients = clientSupplier.get();
        } catch (Exception e) {
            log.error("Failed to supply new S3 clients during pool rebuild", e);
            throw new RuntimeException("Rebuild failed", e);
        }

        S3Client[] oldMeta = this.metadataClients;
        S3Client[] oldData = this.dataClients;

        this.metadataClients = newClients.metadataClients.toArray(new S3Client[0]);
        this.dataClients = newClients.dataClients.toArray(new S3Client[0]);

        // Update MBean standard attributes
        this.poolRebuildCount.incrementAndGet();
        this.lastRebuildTime = java.time.Instant.now().toString();

        stickyRoutes.clear();
        quarantinedNodes.clear();

        // Asynchronously close old clients to avoid blocking the caller
        CompletableFuture.runAsync(() -> {
            log.info("Draining old S3 clients...");
            for (S3Client c : oldMeta)
                closeQuietly(c);
            for (S3Client c : oldData)
                closeQuietly(c);
            log.info("Old S3 clients successfully closed.");
        });

        log.info("S3 connection pools rebuilt successfully.");
    }

    @Override
    public long getPoolRebuildCount() {
        return poolRebuildCount.get();
    }

    @Override
    public String getLastRebuildTime() {
        return lastRebuildTime;
    }
}
