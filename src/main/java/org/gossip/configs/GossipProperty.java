package org.gossip.configs;

import java.io.Serializable;
import java.time.Duration;

public class GossipProperty implements Serializable {
    private final Duration failureTimeout, cleanupTimeout, updateFrequency, detectionInterval;
    private final int peerCount;

    public GossipProperty(Duration failureTimeout,
                          Duration cleanupTimeout,
                          Duration updateFrequency,
                          Duration detectionInterval,
                          int peerCount) {
        this.cleanupTimeout = cleanupTimeout;
        this.failureTimeout = failureTimeout;
        this.updateFrequency = updateFrequency;
        this.detectionInterval = detectionInterval;
        this.peerCount = peerCount;
    }

    public Duration getCleanupTimeout() {
        return cleanupTimeout;
    }

    public Duration getFailureTimeout() {
        return failureTimeout;
    }


    public Duration getDetectionInterval() {
        return detectionInterval;
    }

    public Duration getUpdateFrequency() {
        return updateFrequency;
    }

    public int getPeerCount() {
        return peerCount;
    }
}
