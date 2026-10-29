/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.metrics.traits.ConnectedServerMetricsMXBean;

/**
 * Carries the identity of the server that provides the streaming connection.
 * <p>
 * A task keeps one meter for its lifetime and hands it to the streaming metrics it creates, so that the server of the
 * previous connection and the number of changes survive the restarts after retriable errors, which create new metrics.
 */
@ThreadSafe
public class ConnectedServerMeter implements ConnectedServerMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectedServerMeter.class);

    private volatile Map<String, String> connectedServer = Map.of();
    // The server of the previous successful connection, to recognize a connection to another server
    private Map<String, String> previousServer;
    private final AtomicLong numberOfConnectedServerChanges = new AtomicLong();

    @Override
    public Map<String, String> getConnectedServer() {
        return connectedServer;
    }

    @Override
    public long getNumberOfConnectedServerChanges() {
        return numberOfConnectedServerChanges.get();
    }

    /**
     * Records that the streaming connection is provided by the given server. A server that differs from the one of the
     * previous successful connection counts as a change. An empty identity means that the server is unknown, and is
     * neither compared nor remembered.
     *
     * @param server the identity of the server; may be empty but not null, and must not contain null values
     */
    public synchronized void connected(Map<String, String> server) {
        connectedServer = Map.copyOf(server);
        if (connectedServer.isEmpty()) {
            return;
        }
        if (previousServer != null && !previousServer.equals(connectedServer)) {
            numberOfConnectedServerChanges.incrementAndGet();
            LOGGER.warn("The streaming connection is now provided by server {} instead of server {}", connectedServer, previousServer);
        }
        previousServer = connectedServer;
    }

    /**
     * Records that the streaming connection was lost. The server of the previous connection is kept, so that the next
     * connection can be compared with it.
     */
    public synchronized void disconnected() {
        connectedServer = Map.of();
    }

    public void reset() {
        numberOfConnectedServerChanges.set(0);
    }
}
