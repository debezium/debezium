/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.logError;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setLogFilesForMining;

import java.math.BigInteger;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.RacCommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.InfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);
    private static final int MAXIMUM_NAME_LENGTH = 30;
    private static final String ALL_COLUMN_LOGGING = "ALL COLUMN LOGGING";

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final JdbcConfiguration jdbcConfiguration;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final ErrorHandler errorHandler;
    private final boolean isContinuousMining;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final OracleConnectorConfig connectorConfig;
    private final Duration archiveLogRetention;
    private final boolean archiveLogOnlyMode;
    private final String archiveDestinationName;

    private Scn startScn;
    private Scn endScn;
    private List<BigInteger> currentRedoLogSequences;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                              OracleConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                              Configuration jdbcConfig, OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.connectorConfig = connectorConfig;
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.errorHandler = errorHandler;
        this.streamingMetrics = streamingMetrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
        this.archiveLogOnlyMode = connectorConfig.isArchiveLogOnlyMode();
        this.archiveDestinationName = connectorConfig.getLogMiningArchiveDestinationName();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context
     *         change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext) {
        try {
            startScn = offsetContext.getScn();

            try (LogWriterFlushStrategy flushStrategy = resolveFlushStrategy()) {
                if (!isContinuousMining && startScn.compareTo(getFirstScnInLogs(jdbcConnection)) < 0) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters(jdbcConnection);
                checkDatabaseAndTableState(jdbcConnection, connectorConfig.getPdbName(), schema);

                try (LogMinerEventProcessor processor = createProcessor(context, partition, offsetContext)) {

                    if (archiveLogOnlyMode && !waitForStartScnInArchiveLogs(context, startScn)) {
                        return;
                    }

                    currentRedoLogSequences = getCurrentRedoLogSequences();
                    initializeRedoLogsForMining(jdbcConnection, false, startScn);

                    while (context.isRunning()) {
                        // Calculate time difference before each mining session to detect time zone offset changes (e.g. DST) on database server
                        streamingMetrics.calculateTimeDifference(getDatabaseSystemTime(jdbcConnection));

                        if (archiveLogOnlyMode && !waitForStartScnInArchiveLogs(context, startScn)) {
                            break;
                        }

                        Instant start = Instant.now();
                        endScn = calculateEndScn(jdbcConnection, startScn, endScn);

                        // This is a small window where when archive log only mode has completely caught up to the last
                        // record in the archive logs that both the start and end values are identical. In this use
                        // case we want to pause and restart the loop waiting for a new archive log before proceeding.
                        if (archiveLogOnlyMode && startScn.equals(endScn)) {
                            pauseBetweenMiningSessions();
                            continue;
                        }

                        flushStrategy.flush(jdbcConnection.getCurrentScn());

                        if (hasLogSwitchOccurred()) {
                            // This is the way to mitigate PGA leaks.
                            // With one mining session, it grows and maybe there is another way to flush PGA.
                            // At this point we use a new mining session
                            endMiningSession(jdbcConnection, offsetContext);
                            initializeRedoLogsForMining(jdbcConnection, true, startScn);

                            processor.abandonTransactions(connectorConfig.getLogMiningTransactionRetention());

                            // This needs to be re-calculated because building the data dictionary will force the
                            // current redo log sequence to be advanced due to a complete log switch of all logs.
                            currentRedoLogSequences = getCurrentRedoLogSequences();
                        }

                        if (context.isRunning()) {
                            startMiningSession(jdbcConnection, startScn, endScn);
                            startScn = processor.process(startScn, endScn);

                            captureSessionMemoryStatistics(jdbcConnection);

                            streamingMetrics.setCurrentBatchProcessingTime(Duration.between(start, Instant.now()));
                            pauseBetweenMiningSessions();
                        }
                    }
                }
            }
        }
        catch (Throwable t) {
            logError(streamingMetrics, "Mining session stopped due to the {}", t);
            errorHandler.setProducerThrowable(t);
        }
        finally {
            LOGGER.info("startScn={}, endScn={}", startScn, endScn);
            LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            LOGGER.info("Offsets: {}", offsetContext);
        }
    }

    private void captureSessionMemoryStatistics(OracleConnection connection) throws SQLException {
        long sessionUserGlobalAreaMemory = connection.getSessionStatisticByName("session uga memory");
        long sessionUserGlobalAreaMaxMemory = connection.getSessionStatisticByName("session uga memory max");
        streamingMetrics.setUserGlobalAreaMemory(sessionUserGlobalAreaMemory, sessionUserGlobalAreaMaxMemory);

        long sessionProcessGlobalAreaMemory = connection.getSessionStatisticByName("session pga memory");
        long sessionProcessGlobalAreaMaxMemory = connection.getSessionStatisticByName("session pga memory max");
        streamingMetrics.setProcessGlobalAreaMemory(sessionProcessGlobalAreaMemory, sessionProcessGlobalAreaMaxMemory);

        final DecimalFormat format = new DecimalFormat("#.##");
        LOGGER.debug("Oracle Session UGA {}MB (max = {}MB), PGA {}MB (max = {}MB)",
                format.format(sessionUserGlobalAreaMemory / 1024.f / 1024.f),
                format.format(sessionUserGlobalAreaMaxMemory / 1024.f / 1024.f),
                format.format(sessionProcessGlobalAreaMemory / 1024.f / 1024.f),
                format.format(sessionProcessGlobalAreaMaxMemory / 1024.f / 1024.f));
    }

    private LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                   OraclePartition partition,
                                                   OracleOffsetContext offsetContext) {
        if (OracleConnectorConfig.LogMiningBufferType.INFINISPAN.equals(connectorConfig.getLogMiningBufferType())) {
            return new InfinispanLogMinerEventProcessor(context, connectorConfig, jdbcConnection, dispatcher,
                    partition, offsetContext, schema, streamingMetrics);
        }
        return new MemoryLogMinerEventProcessor(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, streamingMetrics);
    }

    /**
     * Gets the first system change number in both archive and redo logs.
     *
     * @param connection database connection, should not be {@code null}
     * @return the oldest system change number
     * @throws SQLException if a database exception occurred
     * @throws DebeziumException if the oldest system change number cannot be found due to no logs available
     */
    private Scn getFirstScnInLogs(OracleConnection connection) throws SQLException {
        String oldestScn = connection.singleOptionalValue(SqlUtils.oldestFirstChangeQuery(archiveLogRetention, archiveDestinationName), rs -> rs.getString(1));
        if (oldestScn == null) {
            throw new DebeziumException("Failed to calculate oldest SCN available in logs");
        }
        LOGGER.trace("Oldest SCN in logs is '{}'", oldestScn);
        return Scn.valueOf(oldestScn);
    }

    private void initializeRedoLogsForMining(OracleConnection connection, boolean postEndMiningSession, Scn startScn) throws SQLException {
        if (!postEndMiningSession) {
            if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                buildDataDictionary(connection);
            }
            if (!isContinuousMining) {
                setLogFilesForMining(connection, startScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName);
            }
        }
        else {
            if (!isContinuousMining) {
                if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    buildDataDictionary(connection);
                }
                setLogFilesForMining(connection, startScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName);
            }
        }

        updateRedoLogMetrics();
    }

    /**
     * Requests Oracle to build the data dictionary.
     *
     * During the build step, Oracle will perform an additional series of redo log switches.
     * Additionally, this call may introduce a delay in delivering incremental changes since the
     * dictionary will need to have statistics gathered, analyzed, and prepared by LogMiner before
     * any redo entries can be mined.
     *
     * This should only be used in conjunction with the mining strategy
     * {@link io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy#CATALOG_IN_REDO}.
     *
     * @param connection database connection
     * @throws SQLException if a database exception occurred
     */
    private void buildDataDictionary(OracleConnection connection) throws SQLException {
        LOGGER.trace("Building data dictionary");
        connection.executeWithoutCommitting("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
    }

    /**
     * Checks whether a database log switch has occurred and updates metrics if so.
     *
     * @return {@code true} if a log switch was detected, otherwise {@code false}
     * @throws SQLException if a database exception occurred
     */
    private boolean hasLogSwitchOccurred() throws SQLException {
        final List<BigInteger> newSequences = getCurrentRedoLogSequences();
        if (!newSequences.equals(currentRedoLogSequences)) {
            LOGGER.debug("Current log sequence(s) is now {}, was {}", newSequences, currentRedoLogSequences);

            currentRedoLogSequences = newSequences;

            final int logSwitchCount = jdbcConnection.queryAndMap(SqlUtils.switchHistoryQuery(archiveDestinationName), rs -> {
                if (rs.next()) {
                    return rs.getInt(2);
                }
                return 0;
            });
            streamingMetrics.setSwitchCount(logSwitchCount);
            return true;
        }

        return false;
    }

    /**
     * Updates the redo log names and statues in the streaming metrics.
     *
     * @throws SQLException if a database exception occurred
     */
    private void updateRedoLogMetrics() throws SQLException {
        final Map<String, String> logStatuses = jdbcConnection.queryAndMap(SqlUtils.redoLogStatusQuery(), rs -> {
            Map<String, String> results = new LinkedHashMap<>();
            while (rs.next()) {
                results.put(rs.getString(1), rs.getString(2));
            }
            return results;
        });

        final Set<String> fileNames = getCurrentRedoLogFiles(jdbcConnection);
        streamingMetrics.setCurrentLogFileName(fileNames);
        streamingMetrics.setRedoLogStatus(logStatuses);
    }

    /**
     * Get a list of all the CURRENT redo log file names.  For Oracle RAC clusters, multiple filenames
     * will be returned, one for each node that participates in the cluster.
     *
     * @param connection database connection, should not be {@code null}
     * @return unique set of all current redo log file names, with full paths, never {@code null}
     * @throws SQLException if a database exception occurred
     */
    private Set<String> getCurrentRedoLogFiles(OracleConnection connection) throws SQLException {
        final Set<String> fileNames = new HashSet<>();
        connection.query(SqlUtils.currentRedoNameQuery(), rs -> {
            while (rs.next()) {
                fileNames.add(rs.getString(1));
            }
        });
        LOGGER.trace("Current redo log filenames: {}", fileNames);
        return fileNames;
    }

    /**
     * Get the current redo log sequence(s).
     *
     * In an Oracle RAC environment, there are multiple current redo logs and therefore this method
     * returns multiple values, each relating to a single RAC node in the Oracle cluster.
     *
     * @return list of sequence numbers
     * @throws SQLException if a database exception occurred
     */
    private List<BigInteger> getCurrentRedoLogSequences() throws SQLException {
        return jdbcConnection.queryAndMap(SqlUtils.currentRedoLogSequenceQuery(), rs -> {
            List<BigInteger> sequences = new ArrayList<>();
            while (rs.next()) {
                sequences.add(new BigInteger(rs.getString(1)));
            }
            return sequences;
        });
    }

    private void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period = Duration.ofMillis(streamingMetrics.getMillisecondToSleepBetweenMiningQuery());
        Metronome.sleeper(period, clock).pause();
    }

    /**
     * Sets the NLS parameters for the mining session.
     *
     * @param connection database connection, should not be {@code null}
     * @throws SQLException if a database exception occurred
     */
    private void setNlsSessionParameters(OracleConnection connection) throws SQLException {
        final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
                + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
                + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
                + "  NLS_NUMERIC_CHARACTERS = '.,'";

        connection.executeWithoutCommitting(NLS_SESSION_PARAMETERS);
        // This is necessary so that TIMESTAMP WITH LOCAL TIME ZONE is returned in UTC
        connection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");
    }

    /**
     * Get the database system time in the database system's time zone.
     *
     * @param connection database connection, should not be {@code null}
     * @return the database system time
     * @throws SQLException if a database exception occurred
     */
    private OffsetDateTime getDatabaseSystemTime(OracleConnection connection) throws SQLException {
        return connection.singleOptionalValue("SELECT SYSTIMESTAMP FROM DUAL", rs -> rs.getObject(1, OffsetDateTime.class));
    }

    /**
     * Starts a new Oracle LogMiner session.
     *
     * When this is called, LogMiner prepares all the necessary state for an upcoming LogMiner view query.
     * If the mining statement defines using DDL tracking, the data dictionary will be mined as a part of
     * this call to prepare DDL tracking state for the upcoming LogMiner view query.
     *
     * @param connection database connection, should not be {@code null}
     * @param startScn mining session's starting system change number (inclusive), should not be {@code null}
     * @param endScn mining session's ending system change number (inclusive), can be {@code null}
     * @throws SQLException if mining session failed to start
     */
    public void startMiningSession(OracleConnection connection, Scn startScn, Scn endScn) throws SQLException {
        LOGGER.trace("Starting mining session startScn={}, endScn={}, strategy={}, continuous={}",
                startScn, endScn, strategy, isContinuousMining);
        try {
            Instant start = Instant.now();
            connection.executeWithoutCommitting(SqlUtils.startLogMinerStatement(startScn, endScn, strategy, isContinuousMining));
            streamingMetrics.addCurrentMiningSessionStart(Duration.between(start, Instant.now()));
        }
        catch (SQLException e) {
            LOGGER.error("Got exception when starting mining session.", e);
            // Capture the database state before throwing the exception up
            LogMinerDatabaseStateWriter.write(connection);
            throw e;
        }
    }

    /**
     * End the current Oracle LogMiner session, if one is in progress.  If the current session does not
     * have an active mining session, a log message is recorded and the method is a no-op.
     *
     * @param connection database connection, should not be {@code null}
     * @param offsetContext connector offset context, should not be {@code null}
     * @throws SQLException if the current mining session cannot be ended gracefully
     */
    public void endMiningSession(OracleConnection connection, OracleOffsetContext offsetContext) throws SQLException {
        try {
            LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                    startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
            connection.executeWithoutCommitting("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
        }
        catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("LogMiner mining session is already closed.");
                return;
            }
            // LogMiner failed to terminate properly, a restart of the connector will be required.
            throw e;
        }
    }

    /**
     * Calculates the mining session's end system change number.
     *
     * This calculation is based upon a sliding window algorithm to where if the connector is falling behind,
     * the mining session's end point will be calculated based on the batch size and either be increased up
     * to the maximum batch size or reduced to as low as the minimum batch size.
     *
     * Additionally, this method calculates and maintains a sliding algorithm for the sleep time between the
     * mining sessions, increasing the pause up to the maximum sleep time if the connector is not behind or
     * is mining too quick and reducing the pause down to the mimum sleep time if the connector has fallen
     * behind and needs to catch-up faster.
     *
     * @param connection database connection, should not be {@code null}
     * @param startScn upcoming mining session's starting change number, should not be {@code null}
     * @param prevEndScn last mining session's ending system change number, can be {@code null}
     * @return the ending system change number to be used for the upcoming mining session, never {@code null}
     * @throws SQLException if the current max system change number cannot be obtained from the database
     */
    private Scn calculateEndScn(OracleConnection connection, Scn startScn, Scn prevEndScn) throws SQLException {
        Scn currentScn = archiveLogOnlyMode
                ? connection.getMaxArchiveLogScn(archiveDestinationName)
                : connection.getCurrentScn();
        streamingMetrics.setCurrentScn(currentScn);

        // Add the current batch size to the starting system change number
        Scn topScnToMine = startScn.add(Scn.valueOf(streamingMetrics.getBatchSize()));

        // Control adjusting batch size
        boolean topMiningScnInFarFuture = false;
        final Scn defaultBatchScn = Scn.valueOf(connectorConfig.getLogMiningBatchSizeDefault());
        if (topScnToMine.subtract(currentScn).compareTo(defaultBatchScn) > 0) {
            streamingMetrics.changeBatchSize(false, connectorConfig.isLobEnabled());
            topMiningScnInFarFuture = true;
        }
        if (currentScn.subtract(topScnToMine).compareTo(defaultBatchScn) > 0) {
            streamingMetrics.changeBatchSize(true, connectorConfig.isLobEnabled());
        }

        // Control sleep time to reduce database impact
        if (currentScn.compareTo(topScnToMine) < 0) {
            if (!topMiningScnInFarFuture) {
                streamingMetrics.changeSleepingTime(true);
            }
            LOGGER.debug("Using current SCN {} as end SCN.", currentScn);
            return currentScn;
        }
        else {
            if (prevEndScn != null && topScnToMine.compareTo(prevEndScn) <= 0) {
                LOGGER.debug("Max batch size too small, using current SCN {} as end SCN.", currentScn);
                return currentScn;
            }
            streamingMetrics.changeSleepingTime(false);
            if (topScnToMine.compareTo(startScn) < 0) {
                LOGGER.debug("Top SCN calculation resulted in end before start SCN, using current SCN {} as end SCN.", currentScn);
                return currentScn;
            }

            if (prevEndScn != null) {
                final Scn deltaScn = currentScn.subtract(prevEndScn);
                if (deltaScn.compareTo(Scn.valueOf(connectorConfig.getLogMiningScnGapDetectionGapSizeMin())) > 0) {
                    Optional<OffsetDateTime> prevEndScnTimestamp = connection.getScnToTimestamp(prevEndScn);
                    if (prevEndScnTimestamp.isPresent()) {
                        Optional<OffsetDateTime> currentScnTimestamp = connection.getScnToTimestamp(currentScn);
                        if (currentScnTimestamp.isPresent()) {
                            long timeDeltaMs = ChronoUnit.MILLIS.between(prevEndScnTimestamp.get(), currentScnTimestamp.get());
                            if (timeDeltaMs < connectorConfig.getLogMiningScnGapDetectionTimeIntervalMaxMs()) {
                                LOGGER.warn("Detected possible SCN gap, using current SCN, startSCN {}, prevEndScn {} timestamp {}, current SCN {} timestamp {}.",
                                        startScn,
                                        prevEndScn, prevEndScnTimestamp.get(), currentScn, currentScnTimestamp.get());
                                return currentScn;
                            }
                        }
                    }
                }
            }

            LOGGER.debug("Using Top SCN calculation {} as end SCN. currentScn {}, startScn {}", topScnToMine, currentScn, startScn);
            return topScnToMine;
        }
    }

    /**
     * Checks and validates the database's supplemental logging configuration as well as the lengths of the
     * table and column names that are part of the database schema.
     *
     * @param connection database connection, should not be {@code null}
     * @param pdbName pluggable database name, can be {@code null} when not using pluggable databases
     * @param schema connector's database schema, should not be {@code null}
     * @throws SQLException if a database exception occurred
     */
    private void checkDatabaseAndTableState(OracleConnection connection, String pdbName, OracleDatabaseSchema schema) throws SQLException {
        final Instant start = Instant.now();
        LOGGER.trace("Checking database and table state, this may take time depending on the size of your schema.");
        try {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }

            // Check if ALL supplemental logging is enabled at the database
            if (!isDatabaseAllSupplementalLoggingEnabled(connection)) {
                // Check if MIN supplemental logging is enabled at the database
                if (!isDatabaseMinSupplementalLoggingEnabled(connection)) {
                    throw new DebeziumException("Supplemental logging not properly configured. "
                            + "Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
                }

                // Check if ALL COLUMNS supplemental logging is enabled for each captured table
                for (TableId tableId : schema.tableIds()) {
                    if (!isTableAllColumnsSupplementalLoggingEnabled(connection, tableId)) {
                        throw new DebeziumException("Supplemental logging not properly configured for table " + tableId + ". "
                                + "Use: ALTER TABLE " + tableId.schema() + "." + tableId.table()
                                + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
                    }
                    final Table table = schema.tableFor(tableId);
                    if (table == null) {
                        // This should never happen; however in the event something would cause it we can
                        // at least get the table identifier thrown in the error to debug from rather
                        // than an erroneous NPE
                        throw new DebeziumException("Unable to find table in relational model: " + tableId);
                    }
                    checkTableColumnNameLengths(table);
                }
            }
            else {
                // ALL supplemental logging is enabled, now check table/column lengths
                for (TableId tableId : schema.tableIds()) {
                    final Table table = schema.tableFor(tableId);
                    if (table == null) {
                        // This should never happen; however in the event something would cause it we can
                        // at least get the table identifier thrown in the error to debug from rather
                        // than an erroneous NPE
                        throw new DebeziumException("Unable to find table in relational model: " + tableId);
                    }
                    checkTableColumnNameLengths(table);
                }
            }
        }
        finally {
            if (pdbName != null) {
                connection.resetSessionToCdb();
            }
        }
        LOGGER.trace("Database and table state check finished after {} ms", Duration.between(start, Instant.now()).toMillis());
    }

    /**
     * Examines the table and column names and logs a warning if any name exceeds {@link #MAXIMUM_NAME_LENGTH}.
     *
     * @param table the table, should not be {@code null}
     */
    private void checkTableColumnNameLengths(Table table) {
        if (table.id().table().length() > MAXIMUM_NAME_LENGTH) {
            LOGGER.warn("Table '{}' won't be captured by Oracle LogMiner because its name exceeds {} characters.",
                    table.id().table(), MAXIMUM_NAME_LENGTH);
        }
        for (Column column : table.columns()) {
            if (column.name().length() > MAXIMUM_NAME_LENGTH) {
                LOGGER.warn("Table '{}' won't be captured by Oracle LogMiner because column '{}' exceeds {} characters.",
                        table.id().table(), column.name(), MAXIMUM_NAME_LENGTH);
            }
        }
    }

    /**
     * Returns whether the database is configured with ALL supplemental logging.
     *
     * @param connection database connection, must not be {@code null}
     * @return true if all supplemental logging is enabled, false otherwise
     * @throws SQLException if a database exception occurred
     */
    private boolean isDatabaseAllSupplementalLoggingEnabled(OracleConnection connection) throws SQLException {
        return connection.queryAndMap(SqlUtils.databaseSupplementalLoggingAllCheckQuery(), rs -> {
            while (rs.next()) {
                if ("YES".equalsIgnoreCase(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Returns whether the database is configured with MIN supplemental logging.
     *
     * @param connection database connection, must not be {@code null}
     * @return true if min supplemental logging is enabled, false otherwise
     * @throws SQLException if a database exception occurred
     */
    private boolean isDatabaseMinSupplementalLoggingEnabled(OracleConnection connection) throws SQLException {
        return connection.queryAndMap(SqlUtils.databaseSupplementalLoggingMinCheckQuery(), rs -> {
            while (rs.next()) {
                if ("YES".equalsIgnoreCase(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Return whether the table is configured with ALL COLUMN supplemental logging.
     *
     * @param connection database connection, must not be {@code null}
     * @param tableId table identifier, must not be {@code null}
     * @return true if all column supplemental logging is enabled, false otherwise
     * @throws SQLException if a database exception occurred
     */
    private boolean isTableAllColumnsSupplementalLoggingEnabled(OracleConnection connection, TableId tableId) throws SQLException {
        // A table can be defined with multiple logging groups, hence why this check needs to iterate
        // multiple returned rows to see whether ALL_COLUMN_LOGGING is part of the set.
        return connection.queryAndMap(SqlUtils.tableSupplementalLoggingCheckQuery(tableId), rs -> {
            while (rs.next()) {
                if (ALL_COLUMN_LOGGING.equals(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Resolves the Oracle LGWR buffer flushing strategy.
     *
     * @return the strategy to be used to flush Oracle's LGWR process, never {@code null}.
     */
    private LogWriterFlushStrategy resolveFlushStrategy() {
        if (connectorConfig.isRacSystem()) {
            return new RacCommitLogWriterFlushStrategy(connectorConfig, jdbcConfiguration, streamingMetrics);
        }
        return new CommitLogWriterFlushStrategy(jdbcConnection);
    }

    /**
     * Waits for the starting system change number to exist in the archive logs before returning.
     *
     * @param context the change event source context
     * @param startScn the starting system change number
     * @return true if the code should continue, false if the code should end.
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the pause between checks is interrupted
     */
    private boolean waitForStartScnInArchiveLogs(ChangeEventSourceContext context, Scn startScn) throws SQLException, InterruptedException {
        boolean showStartScnNotInArchiveLogs = true;
        while (context.isRunning() && !isStartScnInArchiveLogs(startScn)) {
            if (showStartScnNotInArchiveLogs) {
                LOGGER.warn("Starting SCN {} is not yet in archive logs, waiting for archive log switch.", startScn);
                showStartScnNotInArchiveLogs = false;
                Metronome.sleeper(connectorConfig.getArchiveLogOnlyScnPollTime(), clock).pause();
            }
        }

        if (!context.isRunning()) {
            return false;
        }

        if (!showStartScnNotInArchiveLogs) {
            LOGGER.info("Starting SCN {} is now available in archive logs, log mining unpaused.", startScn);
        }
        return true;
    }

    /**
     * Returns whether the starting system change number is in the archive logs.
     *
     * @param startScn the starting system change number
     * @return true if the starting system change number is in the archive logs; false otherwise.
     * @throws SQLException if a database exception occurred
     */
    private boolean isStartScnInArchiveLogs(Scn startScn) throws SQLException {
        List<LogFile> logs = LogMinerHelper.getLogFilesForOffsetScn(jdbcConnection, startScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName);
        return logs.stream()
                .anyMatch(l -> l.getFirstScn().compareTo(startScn) <= 0 && l.getNextScn().compareTo(startScn) > 0 && l.getType().equals(LogFile.Type.ARCHIVE));
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
