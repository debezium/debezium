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
import io.debezium.util.LoggingContext.PreviousContext;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 *
 * @see MySqlConnector
 * @author Randall Hauch
 */
@NotThreadSafe
public final class MySqlConnectorTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile MySqlTaskContext taskContext;
    private volatile ChainedReader readers;

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
    public synchronized void start(Map<String, String> props) {
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(MySqlConnectorConfig.ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        // Create and start the task context ...
        this.taskContext = new MySqlTaskContext(config);
        PreviousContext prevLoggingContext = this.taskContext.configureLoggingContext("task");
        try {
            this.taskContext.start();

            // Get the offsets for our partition ...
            boolean startWithSnapshot = false;
            boolean snapshotEventsAreInserts = true;
            final SourceInfo source = taskContext.source();
            Map<String, ?> offsets = context.offsetStorageReader().offset(taskContext.source().partition());
            if (offsets != null) {
                // Set the position in our source info ...
                source.setOffset(offsets);
                logger.info("Found existing offset: {}", offsets);

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
                    logger.info("Prior execution was an incomplete snapshot, so starting new snapshot");
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
                    logger.info("Found no existing offset and snapshots disallowed, so starting at beginning of binlog");
                    source.setBinlogStartPoint("", 0L);// start from the beginning of the binlog
                    taskContext.initializeHistory();

                    // Look to see what the first available binlog file is called, and whether it looks like binlog files have
                    // been purged. If so, then output a warning ...
                    String earliestBinlogFilename = earliestBinlogFilename();
                    if (earliestBinlogFilename == null) {
                        logger.warn("No binlog appears to be available. Ensure that the MySQL row-level binlog is enabled.");
                    } else if (!earliestBinlogFilename.endsWith("00001")) {
                        logger.warn("It is possible the server has purged some binlogs. If this is the case, then using snapshot mode may be required.");
                    }
                } else {
                    // We are allowed to use snapshots, and that is the best way to start ...
                    startWithSnapshot = true;
                    // The snapshot will determine if GTIDs are set
                    logger.info("Found no existing offset, so preparing to perform a snapshot");
                    // The snapshot will also initialize history ...
                }
            }

            if (!startWithSnapshot && source.gtidSet() == null && isGtidModeEnabled()) {
                // The snapshot will properly determine the GTID set, but we're not starting with a snapshot and GTIDs were not
                // previously used but the MySQL server has them enabled ...
                source.setCompletedGtidSet("");
            }

            // Check whether the row-level binlog is enabled ...
            final boolean rowBinlogEnabled = isRowBinlogEnabled();

            // Set up the readers, with a callback to `completeReaders` so that we know when it is finished ...
            readers = new ChainedReader();
            readers.uponCompletion(this::completeReaders);
            BinlogReader binlogReader = new BinlogReader("binlog", taskContext);
            if (startWithSnapshot) {
                // We're supposed to start with a snapshot, so set that up ...
                SnapshotReader snapshotReader = new SnapshotReader("snapshot", taskContext);
                snapshotReader.useMinimalBlocking(taskContext.useMinimalSnapshotLocking());
                if (snapshotEventsAreInserts) snapshotReader.generateInsertEvents();
                readers.add(snapshotReader);

                if (taskContext.isInitialSnapshotOnly()) {
                    logger.warn("This connector will only perform a snapshot, and will stop after that completes.");
                    readers.add(new BlockingReader("blocker"));
                    readers.uponCompletion("Connector configured to only perform snapshot, and snapshot completed successfully. Connector will terminate.");
                } else {
                    if (!rowBinlogEnabled) {
                        throw new ConnectException("The MySQL server is not configured to use a row-level binlog, which is "
                                + "required for this connector to work properly. Change the MySQL configuration to use a "
                                + "row-level binlog and restart the connector.");
                    }
                    readers.add(binlogReader);
                }
            } else {
                if (!rowBinlogEnabled) {
                    throw new ConnectException(
                            "The MySQL server does not appear to be using a row-level binlog, which is required for this connector to work properly. Enable this mode and restart the connector.");
                }
                // We're going to start by reading the binlog ...
                readers.add(binlogReader);
            }

            // And finally initialize and start the chain of readers ...
            this.readers.initialize();
            this.readers.start();
        } catch (Throwable e) {
            // If we don't complete startup, then Kafka Connect will not attempt to stop the connector. So if we
            // run into a problem, we have to stop ourselves ...
            try {
                stop();
            } catch (Throwable s) {
                // Log, but don't propagate ...
                logger.error("Failed to start the connector (see other exception), but got this error while cleaning up", s);
            }
            if (e instanceof InterruptedException) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while starting the connector", e);
            }
            if (e instanceof ConnectException) {
                throw (ConnectException) e;
            }
            throw new ConnectException(e);
        } finally {
            prevLoggingContext.restore();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Reader currentReader = readers;
        if (currentReader == null) {
            return null;
        }
        PreviousContext prevLoggingContext = this.taskContext.configureLoggingContext("task");
        try {
            logger.trace("Polling for events");
            return currentReader.poll();
        } finally {
            prevLoggingContext.restore();
        }
    }

    @Override
    public synchronized void stop() {
        if (context != null) {
            PreviousContext prevLoggingContext = this.taskContext.configureLoggingContext("task");
            try {
                logger.info("Stopping MySQL connector task");

                if (readers != null) {
                    readers.stop();
                }
            } finally {
                prevLoggingContext.restore();
            }
        }
    }

    /**
     * When the task is {@link #stop() stopped}, the readers may have additional work to perform before they actually
     * stop and before all their records have been consumed via the {@link #poll()} method. This method signals that
     * all of this has completed.
     */
    protected void completeReaders() {
        PreviousContext prevLoggingContext = this.taskContext.configureLoggingContext("task");
        try {
            // Flush and stop database history, close all JDBC connections ...
            if (this.taskContext != null) taskContext.shutdown();
        } catch (Throwable e) {
            logger.error("Unexpected error shutting down the database history and/or closing JDBC connections", e);
        } finally {
            context = null;
            logger.info("Connector task finished all work and is now shutdown");
            prevLoggingContext.restore();
        }
    }

    /**
     * Determine whether the binlog position as set on the {@link MySqlTaskContext#source() SourceInfo} is available in the
     * server.
     *
     * @return {@code true} if the server has the binlog coordinates, or {@code false} otherwise
     */
    protected boolean isBinlogAvailable() {
        String gtidStr = taskContext.source().gtidSet();
        if (gtidStr != null) {
            if (gtidStr.trim().isEmpty()) return true; // start at beginning ...
            String availableGtidStr = taskContext.knownGtidSet();
            if (availableGtidStr == null || availableGtidStr.trim().isEmpty()) {
                // Last offsets had GTIDs but the server does not use them ...
                logger.info("Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
                return false;
            }
            // GTIDs are enabled, and we used them previously, but retain only those GTID ranges for the allowed source UUIDs ...
            GtidSet gtidSet = new GtidSet(gtidStr).retainAll(taskContext.gtidSourceFilter());
            // Get the GTID set that is available in the server ...
            GtidSet availableGtidSet = new GtidSet(availableGtidStr);
            if (gtidSet.isContainedWithin(availableGtidSet)) {
                logger.info("MySQL current GTID set {} does contain the GTID set required by the connector {}", availableGtidSet, gtidSet);
                return true;
            }
            logger.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
            return false;
        }

        String binlogFilename = taskContext.source().binlogFilename();
        if (binlogFilename == null) return true; // start at current position
        if (binlogFilename.equals("")) return true; // start at beginning

        // Accumulate the available binlog filenames ...
        List<String> logNames = new ArrayList<>();
        try {
            logger.info("Step 0: Get all known binlogs from MySQL");
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
        if (!found) {
            logger.info("Connector requires binlog file '{}', but MySQL only has {}", binlogFilename, String.join(", ", logNames));
        }
        logger.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        return found;
    }

    /**
     * Determine the earliest binlog filename that is still available in the server.
     *
     * @return the name of the earliest binlog filename, or null if there are none.
     */
    protected String earliestBinlogFilename() {
        // Accumulate the available binlog filenames ...
        List<String> logNames = new ArrayList<>();
        try {
            logger.info("Checking all known binlogs from MySQL");
            taskContext.jdbc().query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        if (logNames.isEmpty()) return null;
        return logNames.get(0);
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
     * Determine whether the MySQL server has the row-level binlog enabled.
     *
     * @return {@code true} if the server's {@code binlog_format} is set to {@code ROW}, or {@code false} otherwise
     */
    protected boolean isRowBinlogEnabled() {
        AtomicReference<String> mode = new AtomicReference<String>("");
        try {
            taskContext.jdbc().query("SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> {
                if (rs.next()) {
                    mode.set(rs.getString(2));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at BINLOG mode: ", e);
        }

        logger.debug("binlog_format={}", mode.get());
        return "ROW".equalsIgnoreCase(mode.get());
    }
}
