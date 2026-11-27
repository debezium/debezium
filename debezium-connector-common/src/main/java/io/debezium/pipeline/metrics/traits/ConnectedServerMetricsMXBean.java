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

    /**
     * @return the observed role of the server that provides the active streaming connection: {@code PRIMARY},
     *         {@code REPLICA}, or {@code UNKNOWN} while disconnected, before the first observation, or when the role
     *         cannot be determined
     */
    String getConnectedServerRole();

    /**
     * @return how the role was determined, for example {@code GROUP_REPLICATION} when the database reports it, or
     *         {@code INFERRED} when the connector inferred it from other facts; empty while the role is unknown
     */
    String getConnectedServerRoleSource();

    /**
     * @return the connector-specific facts about the server that the role was determined from, such as whether it is
     *         read-only; empty while disconnected or before the first observation
     */
    Map<String, String> getConnectedServerState();

    /**
     * @return the number of milliseconds since the role of the server was last observed, or {@code -1} if it was not
     *         observed since the connection was established
     */
    long getMilliSecondsSinceConnectedServerRoleObserved();
}
