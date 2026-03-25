package io.github.has3a;

import java.util.List;

/**
 * Immutable point-in-time snapshot of {@link SmartS3ClientProxy} counters.
 * <p>
 * Obtain via {@link SmartS3ClientProxy#getStats()}.
 *
 * @param totalRequests   total number of top-level S3 method invocations
 * @param failoverCount   number of individual attempt failures that triggered a failover
 * @param errorCount      number of invocations where all nodes failed
 * @param nodeStats       per-node statistics, indexed by node position
 */
public record ProxyStats(
        long totalRequests,
        long failoverCount,
        long errorCount,
        List<NodeStats> nodeStats
) {

    /**
     * Per-node statistics snapshot.
     *
     * @param nodeIndex      zero-based node position
     * @param quarantineCount number of times this node was quarantined since proxy creation
     * @param quarantined    whether the node is currently quarantined at snapshot time
     */
    public record NodeStats(
            int     nodeIndex,
            long    quarantineCount,
            boolean quarantined
    ) {}
}
