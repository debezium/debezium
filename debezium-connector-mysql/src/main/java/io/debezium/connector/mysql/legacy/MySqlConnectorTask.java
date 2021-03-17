/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 *
 * @see MySqlConnector
 * @author Randall Hauch
 */
@NotThreadSafe
public final class MySqlConnectorTask extends BaseSourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile MySqlTaskContext taskContext;
    private volatile MySqlJdbcContext connectionContext;
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
    public ChangeEventSourceCoordinator start(Configuration config) {
        final String serverName = config.getString(MySqlConnectorConfig.SERVER_NAME);
        PreviousContext prevLoggingContext = LoggingContext.forConnector(Module.contextName(), serverName, "task");

        try {
            // Get the offsets for our partition ...
            boolean startWithSnapshot = false;
            Map<String, String> partition = Collect.hashMapOf(SourceInfo.SERVER_PARTITION_KEY, serverName);
            Map<String, ?> offsets = getRestartOffset(context.offsetStorageReader().offset(partition));
            final SourceInfo source;
            if (offsets != null) {
                Filters filters = SourceInfo.offsetsHaveFilterInfo(offsets) ? getOldFilters(offsets, config) : getAllFilters(config);
                this.taskContext = createAndStartTaskContext(config, filters);
                this.connectionContext = taskContext.getConnectionContext();
                source = taskContext.source();
                // Set the position in our source info ...
                source.setOffset(offsets);
                logger.info("Found existing offset: {}", offsets);

                // First check if db history is available
                if (!taskContext.historyExists()) {
                    if (taskContext.isSchemaOnlyRecoverySnapshot()) {
                        startWithSnapshot = true;

                        // But check to see if the server still has those binlog coordinates ...
                        if (!isBinlogAvailable()) {
                            String msg = "The connector is trying to read binlog starting at " + source + ", but this is no longer "
                                    + "available on the server. Reconfigure the connector to use a snapshot when needed.";
                            throw new ConnectException(msg);
                        }
                        logger.info("The db-history topic is missing but we are in {} snapshot mode. " +
                                "Attempting to snapshot the current schema and then begin reading the binlog from the last recorded offset.",
                                SnapshotMode.SCHEMA_ONLY_RECOVERY);
                    }
                    else {
                        String msg = "The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                                + SnapshotMode.SCHEMA_ONLY_RECOVERY;
                        throw new ConnectException(msg);
                    }
                    taskContext.initializeHistoryStorage();
                }
                else {

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
                    }
                    else {
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
                }

            }
            else {
                // We have no recorded offsets ...
                this.taskContext = createAndStartTaskContext(config, getAllFilters(config));
                taskContext.initializeHistoryStorage();
                this.connectionContext = taskContext.getConnectionContext();
                source = taskContext.source();

                if (taskContext.isSnapshotNeverAllowed()) {
                    // We're not allowed to take a snapshot, so instead we have to assume that the binlog contains the
                    // full history of the database.
                    logger.info("Found no existing offset and snapshots disallowed, so starting at beginning of binlog");
                    source.setBinlogStartPoint("", 0L); // start from the beginning of the binlog
                    taskContext.initializeHistory();

                    // Look to see what the first available binlog file is called, and whether it looks like binlog files have
                    // been purged. If so, then output a warning ...
                    String earliestBinlogFilename = earliestBinlogFilename();
                    if (earliestBinlogFilename == null) {
                        logger.warn("No binlog appears to be available. Ensure that the MySQL row-level binlog is enabled.");
                    }
                    else if (!earliestBinlogFilename.endsWith("00001")) {
                        logger.warn("It is possible the server has purged some binlogs. If this is the case, then using snapshot mode may be required.");
                    }
                }
                else {
                    // We are allowed to use snapshots, and that is the best way to start ...
                    startWithSnapshot = true;
                    // The snapshot will determine if GTIDs are set
                    logger.info("Found no existing offset, so preparing to perform a snapshot");
                    // The snapshot will also initialize history ...
                }
            }

            if (!startWithSnapshot && source.gtidSet() == null && connectionContext.isGtidModeEnabled()) {
                // The snapshot will properly determine the GTID set, but we're not starting with a snapshot and GTIDs were not
                // previously used but the MySQL server has them enabled ...
                source.setCompletedGtidSet("");
            }

            // Check whether the row-level binlog is enabled ...
            final boolean binlogFormatRow = isBinlogFormatRow();
            final boolean binlogRowImageFull = isBinlogRowImageFull();
            final boolean rowBinlogEnabled = binlogFormatRow && binlogRowImageFull;

            ChainedReader.Builder chainedReaderBuilder = new ChainedReader.Builder();

            // Set up the readers, with a callback to `completeReaders` so that we know when it is finished ...
            if (startWithSnapshot) {
                // We're supposed to start with a snapshot, so set that up ...
                SnapshotReader snapshotReader = new SnapshotReader("snapshot", taskContext);

                snapshotReader.generateReadEvents();

                if (!taskContext.getConnectorConfig().getSnapshotDelay().isZero()) {
                    // Adding a timed blocking reader to delay the snapshot, can help to avoid initial rebalancing interruptions
                    chainedReaderBuilder.addReader(new TimedBlockingReader("timed-blocker", taskContext.getConnectorConfig().getSnapshotDelay()));
                }
                chainedReaderBuilder.addReader(snapshotReader);

                if (taskContext.isInitialSnapshotOnly()) {
                    logger.warn("This connector will only perform a snapshot, and will stop after that completes.");
                    chainedReaderBuilder.addReader(new BlockingReader("blocker",
                            "Connector has completed all of its work but will continue in the running state. It can be shut down at any time."));
                    chainedReaderBuilder
                            .completionMessage("Connector configured to only perform snapshot, and snapshot completed successfully. Connector will terminate.");
                }
                else {
                    if (!rowBinlogEnabled) {
                        if (!binlogFormatRow) {
                            throw new ConnectException("The MySQL server is not configured to use a ROW binlog_format, which is "
                                    + "required for this connector to work properly. Change the MySQL configuration to use a "
                                    + "binlog_format=ROW and restart the connector.");
                        }
                        else {
                            throw new ConnectException("The MySQL server is not configured to use a FULL binlog_row_image, which is "
                                    + "required for this connector to work properly. Change the MySQL configuration to use a "
                                    + "binlog_row_image=FULL and restart the connector.");
                        }
                    }
                    BinlogReader binlogReader = new BinlogReader("binlog", taskContext, null);
                    chainedReaderBuilder.addReader(binlogReader);
                }
            }
            else {
                source.maybeSetFilterDataFromConfig(config);
                if (!rowBinlogEnabled) {
                    throw new ConnectException(
                            "The MySQL server does not appear to be using a full row-level binlog, which is required for this connector to work properly. Enable this mode and restart the connector.");
                }

                // if there are new tables
                if (newTablesInConfig()) {
                    // and we are configured to run a parallel snapshot
                    if (taskContext.getConnectorConfig().getSnapshotNewTables() == MySqlConnectorConfig.SnapshotNewTables.PARALLEL) {
                        ServerIdGenerator serverIdGenerator = new ServerIdGenerator(config.getLong(MySqlConnectorConfig.SERVER_ID),
                                config.getLong(MySqlConnectorConfig.SERVER_ID_OFFSET));
                        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(config,
                                taskContext,
                                getNewFilters(offsets, config),
                                serverIdGenerator);

                        MySqlTaskContext unifiedTaskContext = createAndStartTaskContext(config, getAllFilters(config));
                        // we aren't completing a snapshot, but we need to make sure the "snapshot" flag is false for this new context.
                        unifiedTaskContext.source().completeSnapshot();
                        BinlogReader unifiedBinlogReader = new BinlogReader("binlog",
                                unifiedTaskContext,
                                null,
                                serverIdGenerator.getConfiguredServerId());
                        ReconcilingBinlogReader reconcilingBinlogReader = parallelSnapshotReader.createReconcilingBinlogReader(unifiedBinlogReader);

                        chainedReaderBuilder.addReader(parallelSnapshotReader);
                        chainedReaderBuilder.addReader(reconcilingBinlogReader);
                        chainedReaderBuilder.addReader(unifiedBinlogReader);

                        unifiedBinlogReader.uponCompletion(unifiedTaskContext::shutdown);
                    }
                }
                else {
                    // We're going to start by reading the binlog ...
                    BinlogReader binlogReader = new BinlogReader("binlog", taskContext, null);
                    chainedReaderBuilder.addReader(binlogReader);
                }

            }

            readers = chainedReaderBuilder.build();
            readers.uponCompletion(this::completeReaders);

            // And finally initialize and start the chain of readers ...
            this.readers.initialize();
            this.readers.start();
        }
        catch (Throwable e) {
            // If we don't complete startup, then Kafka Connect will not attempt to stop the connector. So if we
            // run into a problem, we have to stop ourselves ...
            try {
                stop();
            }
            catch (Throwable s) {
                // Log, but don't propagate ...
                logger.error("Failed to start the connector (see other exception), but got this error while cleaning up", s);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Interrupted while starting the connector", e);
            }
            if (e instanceof ConnectException) {
                throw (ConnectException) e;
            }
            throw new ConnectException(e);
        }
        finally {
            prevLoggingContext.restore();
        }

        return null;
    }

    public class ServerIdGenerator {

        private final long configuredServerId;
        private final long offset;
        private int counter;

        private ServerIdGenerator(long configuredServerId, long configuredOffset) {
            this.configuredServerId = configuredServerId;
            this.offset = configuredOffset;
            this.counter = 0;
        }

        public long getNextServerId() {
            counter++;
            return configuredServerId + (counter * offset);
        }

        public long getConfiguredServerId() {
            return configuredServerId;
        }
    }

    /**
     * Get the offset to restart the connector from. Normally, this is just the stored offset.
     *
     * However, if we were doing a parallel load with new tables, it's possible that the last
     * committed offset is from reading the new tables, which could be beyond where we want to
     * restart from (and restarting there could cause skipped events). To fix this, the new
     * tables binlog reader records extra information in its offset to tell the connector where
     * to restart from. If this extra information is present in the stored offset, that is the
     * offset that is returned.
     * @param storedOffset the stored offset.
     * @return the offset to restart from.
     * @see RecordMakers#RecordMakers(MySqlSchema, SourceInfo, TopicSelector, boolean, Map)
     */
    private Map<String, ?> getRestartOffset(Map<String, ?> storedOffset) {
        Map<String, Object> restartOffset = new HashMap<>();
        if (storedOffset != null) {
            for (Entry<String, ?> entry : storedOffset.entrySet()) {
                if (entry.getKey().startsWith(SourceInfo.RESTART_PREFIX)) {
                    String newKey = entry.getKey().substring(SourceInfo.RESTART_PREFIX.length());
                    restartOffset.put(newKey, entry.getValue());
                }
            }
        }
        return restartOffset.isEmpty() ? storedOffset : restartOffset;
    }

    private static MySqlTaskContext createAndStartTaskContext(Configuration config,
                                                              Filters filters) {
        MySqlTaskContext taskContext = new MySqlTaskContext(config, filters);
        taskContext.start();
        return taskContext;
    }

    /**
     * @return true if new tables appear to have been added to the config, and false otherwise.
     */
    private boolean newTablesInConfig() {
        final String elementSep = "/s*,/s*";

        // take in two stringified lists, and return true if the first list contains elements that are not in the second list
        BiFunction<String, String, Boolean> hasExclusiveElements = (String a, String b) -> {
            if (a == null || a.isEmpty()) {
                return false;
            }
            else if (b == null || b.isEmpty()) {
                return true;
            }
            Set<String> bSet = Stream.of(b.split(elementSep)).collect(Collectors.toSet());
            return !Stream.of(a.split(elementSep)).filter((x) -> !bSet.contains(x)).collect(Collectors.toSet()).isEmpty();
        };

        final SourceInfo sourceInfo = taskContext.source();
        final Configuration config = taskContext.config();
        if (!sourceInfo.hasFilterInfo()) {
            // if there was previously no filter info, then we either can't evaluate if there are new tables,
            // or there aren't any new tables because we previously used no filter.
            return false;
        }
        // otherwise, we have filter info
        // if either include lists has been added to, then we may have new tables

        if (hasExclusiveElements.apply(
                config.getFallbackStringProperty(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, MySqlConnectorConfig.DATABASE_WHITELIST),
                sourceInfo.getDatabaseIncludeList())) {
            return true;
        }
        if (hasExclusiveElements.apply(
                config.getFallbackStringProperty(MySqlConnectorConfig.TABLE_INCLUDE_LIST, MySqlConnectorConfig.TABLE_WHITELIST),
                sourceInfo.getTableIncludeList())) {
            return true;
        }
        // if either blacklist has been removed from, then we may have new tables
        if (hasExclusiveElements.apply(sourceInfo.getDatabaseExcludeList(),
                config.getFallbackStringProperty(MySqlConnectorConfig.DATABASE_EXCLUDE_LIST, MySqlConnectorConfig.DATABASE_BLACKLIST))) {
            return true;
        }
        if (hasExclusiveElements.apply(sourceInfo.getTableExcludeList(),
                config.getFallbackStringProperty(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, MySqlConnectorConfig.TABLE_BLACKLIST))) {
            return true;
        }
        // otherwise, false.
        return false;
    }

    /**
     * Get the filters representing the tables that have been newly added to the config, but
     * not those that previously existed in the config.
     * @return {@link Filters}
     */
    private static Filters getNewFilters(Map<String, ?> offsets, Configuration config) {
        Filters oldFilters = getOldFilters(offsets, config);
        return new Filters.Builder(config).excludeAllTables(oldFilters).build();
    }

    /**
     * Get the filters representing those tables that previously existed in the config, but
     * not those newly added to the config.
     * @return {@link Filters}
     */
    private static Filters getOldFilters(Map<String, ?> offsets, Configuration config) {
        return new Filters.Builder(config).setFiltersFromOffsets(offsets).build();
    }

    /**
     * Get the filters representing all tables represented by the config.
     * @return {@link Filters}
     */
    private static Filters getAllFilters(Configuration config) {
        return new Filters.Builder(config).build();
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        Reader currentReader = readers;
        if (currentReader == null) {
            return null;
        }
        PreviousContext prevLoggingContext = this.taskContext.configureLoggingContext("task");
        try {
            logger.trace("Polling for events");
            return currentReader.poll();
        }
        finally {
            prevLoggingContext.restore();
        }
    }

    @Override
    protected void doStop() {
        if (context != null) {
            PreviousContext prevLoggingContext = null;
            if (this.taskContext != null) {
                prevLoggingContext = this.taskContext.configureLoggingContext("task");
            }
            try {
                logger.info("Stopping MySQL connector task");

                if (readers != null) {
                    readers.stop();
                    readers.destroy();
                }
            }
            finally {
                if (prevLoggingContext != null) {
                    prevLoggingContext.restore();
                }
            }
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
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
            if (this.taskContext != null) {
                taskContext.shutdown();
            }
        }
        catch (Throwable e) {
            logger.error("Unexpected error shutting down the database history and/or closing JDBC connections", e);
        }
        finally {
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
            if (gtidStr.trim().isEmpty()) {
                return true; // start at beginning ...
            }
            String availableGtidStr = connectionContext.knownGtidSet();
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
                final GtidSet knownServerSet = availableGtidSet.retainAll(taskContext.gtidSourceFilter());
                final GtidSet gtidSetToReplicate = connectionContext.subtractGtidSet(knownServerSet, gtidSet);
                final GtidSet purgedGtidSet = connectionContext.purgedGtidSet();
                final GtidSet nonPurgedGtidSetToReplicate = connectionContext.subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
                logger.info("GTIDs known by the server but not processed yet {}, for replication are available only {}", gtidSetToReplicate, nonPurgedGtidSetToReplicate);
                if (!gtidSetToReplicate.equals(nonPurgedGtidSetToReplicate)) {
                    logger.info("Some of the GTIDs needed to replicate have been already purged");
                    return false;
                }
                return true;
            }
            logger.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
            return false;
        }

        String binlogFilename = taskContext.source().binlogFilename();
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }

        // Accumulate the available binlog filenames ...
        List<String> logNames = new ArrayList<>();
        try {
            logger.info("Step 0: Get all known binlogs from MySQL");
            connectionContext.jdbc().query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        // And compare with the one we're supposed to use ...
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if (!found) {
            if (logger.isInfoEnabled()) {
                logger.info("Connector requires binlog file '{}', but MySQL only has {}", binlogFilename, String.join(", ", logNames));
            }
        }
        else {
            logger.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }

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
            connectionContext.jdbc().query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        if (logNames.isEmpty()) {
            return null;
        }
        return logNames.get(0);
    }

    /**
     * Determine whether the MySQL server has the binlog_row_image set to 'FULL'.
     *
     * @return {@code true} if the server's {@code binlog_row_image} is set to {@code FULL}, or {@code false} otherwise
     */
    protected boolean isBinlogRowImageFull() {
        AtomicReference<String> rowImage = new AtomicReference<String>("");
        try {
            connectionContext.jdbc().query("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'", rs -> {
                if (rs.next()) {
                    rowImage.set(rs.getString(2));
                }
                else {
                    // This setting was introduced in MySQL 5.6+ with default of 'FULL'.
                    // For older versions, assume 'FULL'.
                    rowImage.set("FULL");
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at BINLOG_ROW_IMAGE mode: ", e);
        }

        logger.debug("binlog_row_image={}", rowImage.get());

        return "FULL".equalsIgnoreCase(rowImage.get());
    }

    /**
     * Determine whether the MySQL server has the row-level binlog enabled.
     *
     * @return {@code true} if the server's {@code binlog_format} is set to {@code ROW}, or {@code false} otherwise
     */
    protected boolean isBinlogFormatRow() {
        AtomicReference<String> mode = new AtomicReference<String>("");
        try {
            connectionContext.jdbc().query("SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> {
                if (rs.next()) {
                    mode.set(rs.getString(2));
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at BINLOG_FORMAT mode: ", e);
        }

        logger.debug("binlog_format={}", mode.get());

        return "ROW".equalsIgnoreCase(mode.get());
    }
}
