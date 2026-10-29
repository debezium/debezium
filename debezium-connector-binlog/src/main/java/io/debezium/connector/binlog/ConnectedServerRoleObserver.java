/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.jdbc.ConnectionFactory;
import io.debezium.pipeline.source.spi.ServerRoleObservation;
import io.debezium.pipeline.source.spi.StreamingProgressListener;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Observes the role of the server that provides the binlog stream, as it can change without the binlog client
 * reconnecting, for example when the server is promoted.
 * <p>
 * The binlog connection accepts no queries once it streams, so the observer queries the server on a connection of its
 * own. As a DNS name or a VIP may resolve that connection to another server, its result is used only when the server
 * id matches the one that the binlog client read on the binlog connection; otherwise the role is reported as unknown.
 * <p>
 * The streaming loop calls {@link #observeIfDue()}, and the binlog client calls {@link #requestObservation()} whenever
 * it connects, so that a new connection is observed without waiting for the interval.
 */
class ConnectedServerRoleObserver implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectedServerRoleObserver.class);

    private final Duration interval;
    private final ConnectionFactory<? extends BinlogConnectorConnection> connectionFactory;
    private final BinaryLogClient client;
    private final StreamingProgressListener listener;
    private final Clock clock;
    private final AtomicBoolean observationRequested = new AtomicBoolean();

    private Timer timer;
    private BinlogConnectorConnection connection;
    // A failure is logged once until an observation succeeds again, as it would otherwise repeat on every interval
    private boolean failureLogged;

    ConnectedServerRoleObserver(Duration interval, ConnectionFactory<? extends BinlogConnectorConnection> connectionFactory,
                                BinaryLogClient client, StreamingProgressListener listener, Clock clock) {
        this.interval = interval;
        this.connectionFactory = connectionFactory;
        this.client = client;
        this.listener = listener;
        this.clock = clock;
    }

    void requestObservation() {
        observationRequested.set(true);
    }

    void observeIfDue() {
        final boolean requested = observationRequested.getAndSet(false);
        if (!requested && timer != null && !timer.expired()) {
            return;
        }
        timer = Threads.timer(clock, interval);
        if (!client.isConnected()) {
            return;
        }
        final long serverId = client.getMasterServerId();
        listener.serverRoleObserved(BinlogStreamingChangeEventSource.serverIdentity(serverId), observe(serverId));
    }

    private ServerRoleObservation observe(long serverId) {
        try {
            if (connection == null) {
                connection = connectionFactory.newConnection();
            }
            final long observedServerId = connection.serverId();
            if (observedServerId != serverId) {
                if (!failureLogged) {
                    LOGGER.warn("Unable to observe the role of server {}, which provides the binlog stream, as the connection for it reached server {}",
                            serverId, observedServerId);
                    failureLogged = true;
                }
                // The next connection may reach the right server
                closeConnection();
                return ServerRoleObservation.UNKNOWN;
            }
            final ServerRoleObservation observation = connection.observeServerRole();
            failureLogged = false;
            return observation;
        }
        catch (SQLException | RuntimeException e) {
            // The observation only serves the metrics, so it must never stop the streaming
            if (!failureLogged) {
                LOGGER.warn("Unable to observe the role of server {}, which provides the binlog stream: {}", serverId, e.getMessage());
                failureLogged = true;
            }
            closeConnection();
            return ServerRoleObservation.UNKNOWN;
        }
    }

    @Override
    public void close() {
        closeConnection();
    }

    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                LOGGER.debug("Unable to close the connection that observes the server role", e);
            }
            connection = null;
        }
    }
}
