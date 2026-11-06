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
import io.debezium.pipeline.source.spi.ServerRole;
import io.debezium.pipeline.source.spi.ServerRoleObservation;
import io.debezium.util.Clock;

/**
 * Carries the identity and the observed role of the server that provides the streaming connection.
 * <p>
 * A task keeps one meter for its lifetime and hands it to the streaming metrics it creates, so that the server of the
 * previous connection and the number of changes survive the restarts after retriable errors, which create new metrics.
 */
@ThreadSafe
public class ConnectedServerMeter implements ConnectedServerMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectedServerMeter.class);

    private final Clock clock;

    private volatile Map<String, String> connectedServer = Map.of();
    // The server of the previous successful connection, to recognize a connection to another server
    private Map<String, String> previousServer;
    private final AtomicLong numberOfConnectedServerChanges = new AtomicLong();

    private volatile ServerRoleObservation role = ServerRoleObservation.UNKNOWN;
    private volatile long roleObservedAt = -1;
    // The last known role is kept to recognize a change of role of the same server, for example a promotion
    private Map<String, String> previousRoleServer;
    private ServerRole previousRole;

    public ConnectedServerMeter() {
        this(Clock.system());
    }

    public ConnectedServerMeter(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Map<String, String> getConnectedServer() {
        return connectedServer;
    }

    @Override
    public long getNumberOfConnectedServerChanges() {
        return numberOfConnectedServerChanges.get();
    }

    @Override
    public String getConnectedServerRole() {
        return role.role().name();
    }

    @Override
    public String getConnectedServerRoleSource() {
        return role.source();
    }

    @Override
    public Map<String, String> getConnectedServerState() {
        return role.state();
    }

    @Override
    public long getMilliSecondsSinceConnectedServerRoleObserved() {
        final long observedAt = roleObservedAt;
        return observedAt < 0 ? -1 : clock.currentTimeInMillis() - observedAt;
    }

    /**
     * Records that the streaming connection is provided by the given server. A server that differs from the one of the
     * previous successful connection counts as a change. An empty identity means that the server is unknown, and is
     * neither compared nor remembered. The role is unknown until it is observed for this connection.
     *
     * @param server the identity of the server; may be empty but not null, and must not contain null values
     */
    public synchronized void connected(Map<String, String> server) {
        connectedServer = Map.copyOf(server);
        clearRole();
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
        clearRole();
    }

    /**
     * Records the observed role of a server. An observation of another server than the one that currently provides the
     * streaming connection is ignored, as it may have been made just before the connector reconnected elsewhere. A role
     * change of the same server does not count as a change of server.
     *
     * @param server the identity of the server that the role was observed for
     * @param observation the observed role
     */
    public synchronized void roleObserved(Map<String, String> server, ServerRoleObservation observation) {
        if (connectedServer.isEmpty() || !connectedServer.equals(server)) {
            return;
        }
        if (observation.role() != ServerRole.UNKNOWN) {
            if (connectedServer.equals(previousRoleServer) && previousRole != observation.role()) {
                LOGGER.info("The role of server {} is now {} instead of {}", connectedServer, observation.role(), previousRole);
            }
            previousRoleServer = connectedServer;
            previousRole = observation.role();
        }
        role = observation;
        roleObservedAt = clock.currentTimeInMillis();
    }

    public void reset() {
        numberOfConnectedServerChanges.set(0);
    }

    private void clearRole() {
        role = ServerRoleObservation.UNKNOWN;
        roleObservedAt = -1;
    }
}
