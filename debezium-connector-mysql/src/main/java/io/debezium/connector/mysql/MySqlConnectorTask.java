/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
@NotThreadSafe
public final class MySqlConnectorTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private MySqlTaskContext taskContext;
    private SnapshotReader snapshotReader;
    private BinlogReader binlogReader;
    private volatile AbstractReader currentReader;

    /**
     * Create an instance of the log reader that uses Kafka to store database schema history and the
     * {@link TopicSelector#defaultSelector(String) default topic selector} of "{@code <serverName>.<databaseName>.<tableName>}"
     * for
     * data and "{@code <serverName>}" for metadata.
     */
    public MySqlConnectorTask() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (!config.validate(MySqlConnectorConfig.ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        // Create and start the task context ...
        this.taskContext = new MySqlTaskContext(config);
        this.taskContext.start();

        // Get the offsets for our partition ...
        boolean startWithSnapshot = false;
        boolean snapshotEventsAreInserts = true;
        final SourceInfo source = taskContext.source();
        Map<String, ?> offsets = context.offsetStorageReader().offset(taskContext.source().partition());
        if (offsets != null) {
            // Set the position in our source info ...
            source.setOffset(offsets);

            // Before anything else, recover the database history to the specified binlog coordinates ...
            taskContext.loadHistory(source);

            if (source.isSnapshotInEffect()) {
                // The last offset was an incomplete snapshot that we cannot recover from...
                if (taskContext.isSnapshotNeverAllowed()) {
                    // No snapshots are allowed
                    String msg = "The connector previously stopped while taking a snapshot, but now the connector is configured "
                            + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.";
                    throw new ConnectException(msg);
                }
                // Otherwise, restart a new snapshot ...
                startWithSnapshot = true;
            } else {
                // No snapshot was in effect, so we should just start reading from the binlog ...
                startWithSnapshot = false;

                // But check to see if the server still has those binlog coordinates ...
                if (!isBinlogAvailable()) {
                    if (!taskContext.isSnapshotAllowedWhenNeeded()) {
                        String msg = "The connector is trying to read binlog starting at " + source + ", but this is no longer "
                                + "available on the server. Reconfigure the connector to use a snapshot when needed.";
                        throw new ConnectException(msg);
                    }
                    startWithSnapshot = true;
                }
            }

        } else {
            // We have no recorded offsets ...
            if (taskContext.isSnapshotNeverAllowed()) {
                // We're not allowed to take a snapshot, so instead we have to assume that the binlog contains the
                // full history of the database.
                source.setBinlogStartPoint("", 0L);// start from the beginning of the binlog
            } else {
                // We are allowed to use snapshots, and that is the best way to start ...
                startWithSnapshot = true;
                // The snapshot will determine if GTIDs are set
            }
        }

        if (!startWithSnapshot && source.gtidSet() == null && isGtidModeEnabled()) {
            // The snapshot will properly determine the GTID set, but we're not starting with a snapshot and GTIDs were not
            // previously used but the MySQL server has them enabled ...
            source.setGtidSet("");
        }

        // Set up the readers ...
        this.binlogReader = new BinlogReader(taskContext);
        if (startWithSnapshot) {
            // We're supposed to start with a snapshot, so set that up ...
            this.snapshotReader = new SnapshotReader(taskContext);
            this.snapshotReader.onSuccessfulCompletion(this::transitionToReadBinlog);
            this.snapshotReader.useMinimalBlocking(taskContext.useMinimalSnapshotLocking());
            if (snapshotEventsAreInserts) this.snapshotReader.generateInsertEvents();
            this.currentReader = this.snapshotReader;
        } else {
            // Just starting to read the binlog ...
            this.currentReader = this.binlogReader;
        }

        // And start our first reader ...
        this.currentReader.start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.trace("Polling for events");
        return currentReader.poll();
    }

    @Override
    public void stop() {
        logger.info("Stopping MySQL connector task");
        // We need to explicitly stop both readers, in this order. If we were to instead call 'currentReader.stop()', there
        // is a chance without synchronization that we'd miss the transition and stop only the snapshot reader. And stopping both
        // is far simpler and more efficient than synchronizing ...
        try {
            this.snapshotReader.stop();
        } finally {
            try {
                this.binlogReader.stop();
            } finally {
                try {
                    // Flush and stop database history, close all JDBC connections ...
                    taskContext.shutdown();
                } catch (Throwable e) {
                    logger.error("Unexpected error shutting down the database history and/or closing JDBC connections", e);
                } finally {
                    logger.info("Connector task successfully stopped");
                }
            }
        }
    }

    protected void transitionToReadBinlog() {
        logger.debug("Transitioning from snapshot reader to binlog reader");
        this.binlogReader.start();
        this.currentReader = this.binlogReader;
    }

    /**
     * Determine whether the binlog position as set on the {@link MySqlTaskContext#source() SourceInfo} is available in the
     * server.
     * 
     * @return {@code true} if the server has the binlog coordinates, or {@code false} otherwise
     */
    protected boolean isBinlogAvailable() {
        String gtidStr = taskContext.source().gtidSet();
        if ( gtidStr != null) {
            if ( gtidStr.trim().isEmpty() ) return true; // start at beginning ...
            String availableGtidStr = knownGtidSet();
            if ( availableGtidStr == null || availableGtidStr.trim().isEmpty() ) {
                // Last offsets had GTIDs but the server does not use them ...
                logger.info("Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
                return false;
            }
            // GTIDs are enabled, and we used them previously ...
            GtidSet gtidSet = new GtidSet(gtidStr);
            GtidSet availableGtidSet = new GtidSet(knownGtidSet());
            if ( gtidSet.isContainedWithin(availableGtidSet)) {
                return true;
            }
            logger.info("Connector last known GTIDs are {}, but MySQL has {}",gtidSet,availableGtidSet);
            return false;
        }
        
        String binlogFilename = taskContext.source().binlogFilename();
        if (binlogFilename == null) return true; // start at current position
        if (binlogFilename.equals("")) return true; // start at beginning

        // Accumulate the available binlog filenames ...
        List<String> logNames = new ArrayList<>();
        try {
            logger.info("Stop 0: Get all known binlogs from MySQL");
            taskContext.jdbc().query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        // And compare with the one we're supposed to use ...
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if ( !found ) {
            logger.info("Connector requires binlog file '{}', but MySQL only has {}",binlogFilename,String.join(", ",logNames));
        }
        return found;
    }

    /**
     * Determine whether the MySQL server has GTIDs enabled.
     * 
     * @return {@code false} if the server's {@code gtid_mode} is set and is {@code OFF}, or {@code true} otherwise
     */
    protected boolean isGtidModeEnabled() {
        AtomicReference<String> mode = new AtomicReference<String>("off");
        try {
            taskContext.jdbc().query("SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'", rs -> {
                if (rs.next()) {
                    mode.set(rs.getString(1));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        return !"OFF".equalsIgnoreCase(mode.get());
    }

    /**
     * Determine the available GTID set for MySQL.
     * 
     * @return the string representation of MySQL's GTID sets.
     */
    protected String knownGtidSet() {
        AtomicReference<String> gtidSetStr = new AtomicReference<String>();
        try {
            taskContext.jdbc().query("SHOW MASTER STATUS", rs -> {
                if (rs.next()) {
                    gtidSetStr.set(rs.getString(5));// GTID set, may be null, blank, or contain a GTID set
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        return gtidSetStr.get();
    }
}
