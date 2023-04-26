/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

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

    private final Map<String, CommitLogWriterFlushStrategy> flushStrategies = new HashMap<>();
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
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
                                           OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConfiguration = jdbcConfig;
        this.streamingMetrics = streamingMetrics;
        this.connectorConfig = connectorConfig;
        this.hosts = connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet());
        recreateRacNodeFlushStrategies();
    }

    @Override
    public void close() {
        closeRacNodeFlushStrategies();
        flushStrategies.clear();
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
        if (flushStrategies.isEmpty()) {
            throw new DebeziumException("No RAC node addresses supplied or currently connected");
        }

        boolean recreateConnections = false;
        for (Map.Entry<String, CommitLogWriterFlushStrategy> entry : flushStrategies.entrySet()) {
            final CommitLogWriterFlushStrategy strategy = entry.getValue();
            try {
                // Flush the node's commit log writer
                strategy.flush(currentScn);
            }
            catch (Exception e) {
                LOGGER.warn("Failed to flush LGWR buffer on RAC node '{}'", strategy.getHost(), e);
                recreateConnections = true;
            }
        }

        if (recreateConnections) {
            recreateRacNodeFlushStrategies();
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

    private void recreateRacNodeFlushStrategies() {
        // Close existing flush strategies to RAC nodes
        closeRacNodeFlushStrategies();

        // Clear map
        flushStrategies.clear();

        // Create strategies by host
        for (String hostName : hosts) {
            try {
                final String[] parts = hostName.split(":");
                flushStrategies.put(hostName, createHostFlushStrategy(parts[0], Integer.parseInt(parts[1])));
            }
            catch (SQLException e) {
                throw new DebeziumException("Cannot connect to RAC node '" + hostName + "'", e);
            }
        }
    }

    private CommitLogWriterFlushStrategy createHostFlushStrategy(String hostName, Integer port) throws SQLException {
        JdbcConfiguration jdbcHostConfig = JdbcConfiguration.adapt(jdbcConfiguration.edit()
                .with(JdbcConfiguration.HOSTNAME, hostName)
                .with(JdbcConfiguration.PORT, port).build());

        LOGGER.debug("Creating flush connection to RAC node '{}'", hostName);
        return new CommitLogWriterFlushStrategy(connectorConfig, jdbcHostConfig);
    }

    /**
     * Closes the RAC node flush strategies.
     */
    private void closeRacNodeFlushStrategies() {
        for (CommitLogWriterFlushStrategy strategy : flushStrategies.values()) {
            try {
                // close the strategy's connection
                strategy.close();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to close RAC connection to node '{}'", strategy.getHost(), e);
                streamingMetrics.incrementWarningCount();
            }
        }
    }
}
