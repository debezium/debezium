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
import io.debezium.config.Configuration;
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

    private final Map<String, CommitLogWriterFlushStrategy> flushStrategies = new HashMap<>();
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
                final String[] parts = parseHostName(hostName);
                flushStrategies.put(hostName, createHostFlushStrategy(parts[0], Integer.parseInt(parts[1]), parts[2]));
            }
            catch (SQLException e) {
                throw new DebeziumException("Cannot connect to RAC node '" + hostName + "'", e);
            }
        }
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

    private CommitLogWriterFlushStrategy createHostFlushStrategy(String hostName, Integer port, String sid) throws SQLException {
        Configuration.Builder jdbcConfigBuilder = jdbcConfiguration.edit()
                .with(JdbcConfiguration.HOSTNAME, hostName)
                .with(JdbcConfiguration.PORT, port);

        if (!Strings.isNullOrBlank(sid)) {
            jdbcConfigBuilder = jdbcConfigBuilder.with(JdbcConfiguration.DATABASE, sid);
        }

        final JdbcConfiguration jdbcHostConfig = JdbcConfiguration.adapt(jdbcConfigBuilder.build());

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
