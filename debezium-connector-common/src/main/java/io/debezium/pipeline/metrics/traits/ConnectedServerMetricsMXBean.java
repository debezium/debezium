/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

import java.util.Map;

/**
 * Exposes the server that provides the streaming connection, as opposed to the configured address, which a DNS name,
 * a VIP or a proxy may resolve to another server after a failover.
 */
public interface ConnectedServerMetricsMXBean {

    /**
     * @return the connector-specific identity of the server that provides the active streaming connection; empty while
     *         disconnected or when the server is unknown
     */
    Map<String, String> getConnectedServer();

    /**
     * @return the number of times a successful connection was observed on another server than the previous one
     */
    long getNumberOfConnectedServerChanges();
}
