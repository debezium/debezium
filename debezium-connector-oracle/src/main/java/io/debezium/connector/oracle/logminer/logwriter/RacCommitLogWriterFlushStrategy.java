/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * A {@link LogWriterFlushStrategy} for Oracle RAC that performs a transaction-scoped commit
 * to flush the Oracle LogWriter (LGWR) process on each RAC node.
 *
 * This strategy builds atop of {@link CommitLogWriterFlushStrategy} by creating a commit strategy
 * to each Oracle RAC node and orchestrating the flushes simultaneously for each node when a flush
 * is needed.  In the event that a node fails to flush, this strategy will delay for 3 seconds to
 * allow Oracle to automatically flush the buffers before proceeding.
 *
 * @author Chris Cranford
 */
public class RacCommitLogWriterFlushStrategy implements LogWriterFlushStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(RacCommitLogWriterFlushStrategy.class);

    private final List<RacNode> racNodes = new ArrayList<>();
    private final LogMinerStreamingChangeEventSourceMetrics streamingMetrics;
    private final JdbcConfiguration jdbcConfiguration;
    private final OracleConnectorConfig connectorConfig;
    private final Set<String> hosts;

    /**
     * Creates an Oracle RAC LogWriter (LGWR) flushing strategy.
     *
     * @param connectorConfig the connector configuration, must not be {@code null}
     * @param jdbcConfig the mining session JDBC connection configuration, must not be {@code null}
     * @param streamingMetrics the streaming metrics, must not be {@code null}
     */
    public RacCommitLogWriterFlushStrategy(OracleConnectorConfig connectorConfig, JdbcConfiguration jdbcConfig,
                                           LogMinerStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConfiguration = jdbcConfig;
        this.streamingMetrics = streamingMetrics;
        this.connectorConfig = connectorConfig;
        this.hosts = connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet());
        createRacNodesList();
    }

    @Override
    public void close() {
        for (RacNode node : racNodes) {
            node.close();
        }
        racNodes.clear();
    }

    @Override
    public String getHost() {
        return String.join(", ", hosts);
    }

    @Override
    public void flush(Scn currentScn) throws InterruptedException {
        // Oracle RAC has one LogWriter (LGWR) process per node (instance).
        // For this configuration, all LGWR processes across all instances must be flushed.
        // Queries cannot be used such as gv_instance as not all nodes could be load balanced.
        Instant startTime = Instant.now();
        if (racNodes.isEmpty()) {
            // In this case it means all nodes have disappeared and main connection will likely throw
            // a database error too, so it's safe to throw here.
            throw new DebeziumException("No RAC node addresses supplied or currently connected");
        }

        // Before flushing, verify that nodes are connected
        for (RacNode node : racNodes) {
            if (!node.isConnected()) {
                // Attempt to reconnect
                node.reconnect();
            }
        }

        // Flush to nodes
        boolean allNodesFlushed = true;
        for (RacNode node : racNodes) {
            if (node.isConnected()) {
                final LogWriterFlushStrategy strategy = node.getFlushStrategy();
                try {
                    // Flush the node's commit log writer
                    strategy.flush(currentScn);
                }
                catch (Exception e) {
                    LOGGER.warn("Failed to flush LGWR buffer on RAC node '{}': {}", node.getHostName(), e.getMessage());
                    node.close();
                    allNodesFlushed = false;
                }
            }
            else {
                allNodesFlushed = false;
            }
        }

        if (!allNodesFlushed) {
            LOGGER.warn("Not all LGWR buffers were flushed, waiting 3 seconds for Oracle to flush automatically.");
            Metronome metronome = Metronome.sleeper(Duration.ofSeconds(3), Clock.SYSTEM);
            try {
                metronome.pause();
            }
            catch (InterruptedException e) {
                LOGGER.warn("The LGWR buffer wait was interrupted.");
                throw e;
            }
        }

        LOGGER.trace("LGWR flush took {} to complete.", Duration.between(startTime, Instant.now()));
    }

    private void createRacNodesList() {
        for (String hostName : hosts) {
            try {
                final RacNode node = new RacNode(hostName);
                racNodes.add(node);

                // This must be done after adding to collection in case connection fails because
                // elastic retries happen during the flush call to this outer RAC strategy.
                node.connect();
            }
            catch (Exception e) {
                LOGGER.warn("Connect to RAC node '{}' failed (will be retried): {}", hostName, e.getMessage());
            }
        }
    }

    private class RacNode {
        private final String hostName;

        private OracleConnection connection;
        private LogWriterFlushStrategy flushStrategy;

        RacNode(String hostName) {
            this.hostName = hostName;
        }

        /**
         * Get the hostname for the RAC node.
         * @return the hostname
         */
        public String getHostName() {
            return this.hostName;
        }

        /**
         * Get the strategy used by the node to flush
         *
         * @return the flush strategy
         */
        public LogWriterFlushStrategy getFlushStrategy() {
            return flushStrategy;
        }

        /**
         * Return whether the RAC node is current connected or not.
         */
        public boolean isConnected() {
            try {
                return connection != null && connection.isConnected();
            }
            catch (SQLException e) {
                return false;
            }
        }

        /**
         * Connects to the RAC node
         *
         * @throws SQLException if a connection failure occurred
         */
        public void connect() throws SQLException {
            final String[] parts = parseHostName(hostName);

            final String databaseHostName = parts[0];
            final int port = Integer.parseInt(parts[1]);
            final String sid = parts[2];

            Configuration.Builder jdbcConfigBuilder = jdbcConfiguration.edit()
                    .with(JdbcConfiguration.HOSTNAME, databaseHostName)
                    .with(JdbcConfiguration.PORT, port);

            if (!Strings.isNullOrBlank(sid)) {
                jdbcConfigBuilder = jdbcConfigBuilder.with(JdbcConfiguration.DATABASE, sid);
            }

            final JdbcConfiguration jdbcHostConfig = JdbcConfiguration.adapt(jdbcConfigBuilder.build());

            this.connection = new OracleConnection(jdbcHostConfig);
            this.connection.setAutoCommit(false);

            LOGGER.info("Created flush connection to RAC node '{}'", hostName);
            this.flushStrategy = new CommitLogWriterFlushStrategy(connectorConfig, connection);
        }

        /**
         * Reconnect to the RAC node
         */
        void reconnect() {
            try {
                if (connection == null) {
                    // Close was called
                    connect();
                }
                else {
                    // Close wasn't called but the connection lost.
                    connection.reconnect();

                    // Recreate the flush strategy if needed
                    if (this.flushStrategy == null) {
                        this.flushStrategy = new CommitLogWriterFlushStrategy(connectorConfig, connection);
                    }
                }
                LOGGER.info("Successfully reconnected to Oracle RAC node '{}'", hostName);
            }
            catch (Exception e) {
                LOGGER.warn("Failed to reconnect to RAC node '{}': {}", hostName, e.getMessage());
                close();
            }
        }

        /**
         * Closes the connection with the RAC node.
         */
        public void close() {
            if (flushStrategy != null) {
                final String hostName = flushStrategy.getHost();
                try {
                    flushStrategy.close();
                }
                catch (Exception e) {
                    LOGGER.warn("Failed to close RAC flush strategy to node '{}'", hostName, e);
                    streamingMetrics.incrementWarningCount();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception e) {
                    LOGGER.warn("Failed to close RAC connection to node '{}'", hostName, e);
                }
            }
            flushStrategy = null;
        }

        private String[] parseHostName(String hostName) {
            final String[] parts = new String[3];
            final String[] colonParts = hostName.split(":");
            parts[0] = colonParts[0];
            if (colonParts[1].contains("/")) {
                // SID provided
                final int slashIndex = colonParts[1].indexOf('/');
                parts[1] = colonParts[1].substring(0, slashIndex);
                parts[2] = colonParts[1].substring(slashIndex + 1);
                return parts;
            }
            else {
                // No SID provided
                parts[1] = colonParts[1];
                parts[2] = null;
            }

            return parts;
        }
    }
}
