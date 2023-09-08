/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.setLogFilesForMining;

import java.math.BigInteger;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.RacCommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.ReadOnlyLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);
    private static final int MAXIMUM_NAME_LENGTH = 30;
    private static final String ALL_COLUMN_LOGGING = "ALL COLUMN LOGGING";
    private static final int MINING_START_RETRIES = 5;
    private static final Long SMALL_REDO_LOG_WARNING = 524_288_000L;

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
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
    private final int logFileQueryMaxRetries;
    private final Duration initialDelay;
    private final Duration maxDelay;

    private Scn startScn; // startScn is the **exclusive** lower bound for mining
    private Scn endScn;
    private Scn snapshotScn;
    private List<LogFile> currentLogFiles;
    private List<BigInteger> currentRedoLogSequences;
    private OracleOffsetContext effectiveOffset;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                              OracleConnection jdbcConnection, EventDispatcher<OraclePartition, TableId> dispatcher,
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
        this.logFileQueryMaxRetries = connectorConfig.getMaximumNumberOfLogQueryRetries();
        this.initialDelay = connectorConfig.getLogMiningInitialDelay();
        this.maxDelay = connectorConfig.getLogMiningMaxDelay();
    }

    @Override
    public void init(OracleOffsetContext offsetContext) throws InterruptedException {
        this.effectiveOffset = offsetContext == null ? emptyContext() : offsetContext;
    }

    private OracleOffsetContext emptyContext() {
        return OracleOffsetContext.create().logicalName(connectorConfig)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>()).build();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context
     *         change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext) {
        if (!connectorConfig.getSnapshotMode().shouldStream()) {
            LOGGER.info("Streaming is not enabled in current configuration");
            return;
        }
        try {

            prepareConnection(false);

            this.effectiveOffset = offsetContext;
            startScn = offsetContext.getScn();
            snapshotScn = offsetContext.getSnapshotScn();
            Scn firstScn = getFirstScnInLogs(jdbcConnection);
            if (startScn.compareTo(snapshotScn) == 0) {
                // This is the initial run of the streaming change event source.
                // We need to compute the correct start offset for mining. That is not the snapshot offset,
                // but the start offset of the oldest transaction that was still pending when the snapshot
                // was taken.
                computeStartScnForFirstMiningSession(offsetContext, firstScn);
            }

            try (LogWriterFlushStrategy flushStrategy = resolveFlushStrategy()) {
                if (!isContinuousMining && startScn.compareTo(firstScn.subtract(Scn.ONE)) < 0) {
                    // startScn is the exclusive lower bound, so must be >= (firstScn - 1)
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                checkDatabaseAndTableState(jdbcConnection, connectorConfig.getPdbName(), schema);

                logOnlineRedoLogSizes(connectorConfig);

                try (LogMinerEventProcessor processor = createProcessor(context, partition, offsetContext)) {

                    if (archiveLogOnlyMode && !waitForStartScnInArchiveLogs(context, startScn)) {
                        return;
                    }

                    initializeRedoLogsForMining(jdbcConnection, false, startScn);

                    int retryAttempts = 1;
                    Stopwatch sw = Stopwatch.accumulating().start();
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

                        final Duration deviation = connectorConfig.getLogMiningMaxScnDeviation();
                        if (!deviation.isZero()) {
                            Optional<Scn> deviatedScn = calculateDeviatedEndScn(startScn, endScn, deviation);
                            if (deviatedScn.isEmpty()) {
                                pauseBetweenMiningSessions();
                                continue;
                            }
                            endScn = deviatedScn.get();
                        }

                        flushStrategy.flush(jdbcConnection.getCurrentScn());

                        boolean restartRequired = false;
                        if (connectorConfig.getLogMiningMaximumSession().isPresent()) {
                            final Duration totalDuration = sw.stop().durations().statistics().getTotal();
                            if (totalDuration.toMillis() >= connectorConfig.getLogMiningMaximumSession().get().toMillis()) {
                                LOGGER.info("LogMiner session has exceeded maximum session time of '{}', forcing restart.", connectorConfig.getLogMiningMaximumSession());
                                restartRequired = true;
                            }
                            else {
                                // resume the existing stop watch, we haven't met the criteria yet
                                sw.start();
                            }
                        }

                        if (restartRequired || hasLogSwitchOccurred()) {
                            // This is the way to mitigate PGA leaks.
                            // With one mining session, it grows and maybe there is another way to flush PGA.
                            // At this point we use a new mining session
                            endMiningSession(jdbcConnection, offsetContext);
                            if (connectorConfig.isLogMiningRestartConnection()) {
                                prepareConnection(true);
                            }
                            initializeRedoLogsForMining(jdbcConnection, true, startScn);

                            // log switch or restart required, re-create a new stop watch
                            sw = Stopwatch.accumulating().start();
                        }

                        if (context.isRunning()) {
                            if (!startMiningSession(jdbcConnection, startScn, endScn, retryAttempts)) {
                                retryAttempts++;
                            }
                            else {
                                retryAttempts = 1;
                                startScn = processor.process(startScn, endScn);
                                streamingMetrics.setCurrentBatchProcessingTime(Duration.between(start, Instant.now()));
                                captureSessionMemoryStatistics(jdbcConnection);
                            }
                            pauseBetweenMiningSessions();
                        }

                        if (context.isPaused()) {
                            LOGGER.info("Streaming will now pause");
                            context.streamingPaused();
                            context.waitSnapshotCompletion();
                            LOGGER.info("Streaming resumed");
                        }

                    }
                }
            }
        }
        catch (Throwable t) {
            LOGGER.error("Mining session stopped due to error.", t);
            streamingMetrics.incrementErrorCount();
            errorHandler.setProducerThrowable(t);
        }
        finally {
            LOGGER.info("startScn={}, endScn={}", startScn, endScn);
            LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            LOGGER.info("Offsets: {}", offsetContext);
        }
    }

    private void prepareConnection(boolean closeAndReconnect) throws SQLException {
        if (closeAndReconnect) {
            // Close and reconnect
            LOGGER.debug("Log switch or maximum session threshold detected, restarting Oracle JDBC connection.");
            jdbcConnection.close();
        }

        // We explicitly expect auto-commit to be disabled
        jdbcConnection.setAutoCommit(false);
        setNlsSessionParameters(jdbcConnection);
    }

    private void logOnlineRedoLogSizes(OracleConnectorConfig config) throws SQLException {
        jdbcConnection.query("SELECT GROUP#, BYTES FROM V$LOG ORDER BY 1", rs -> {
            LOGGER.info("Redo Log Group Sizes:");
            boolean potentiallySmallLogs = false;
            while (rs.next()) {
                long logSize = rs.getLong(2);
                if (logSize < SMALL_REDO_LOG_WARNING) {
                    potentiallySmallLogs = true;
                }
                LOGGER.info("\tGroup #{}: {} bytes", rs.getInt(1), logSize);
            }
            if (config.getAdapter().getType().equals(LogMinerAdapter.TYPE)) {
                if (config.getLogMiningStrategy() == OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO) {
                    if (potentiallySmallLogs) {
                        LOGGER.warn("Redo logs may be sized too small using the default mining strategy, " +
                                "consider increasing redo log sizes to a minimum of 500MB.");
                    }
                }
            }
        });
    }

    /**
     * Computes the start SCN for the first mining session.
     *
     * Normally, this would be the snapshot SCN, but if there were pending transactions at the time
     * the snapshot was taken, we'd miss the events in those transactions that have an SCN smaller
     * than the snapshot SCN.
     *
     * @param offsetContext the offset context
     * @param firstScn the oldest SCN still available in the REDO logs
     */
    private void computeStartScnForFirstMiningSession(OracleOffsetContext offsetContext, Scn firstScn) {
        // This is the initial run of the streaming change event source.
        // We need to compute the correct start offset for mining. That is not the snapshot offset,
        // but the start offset of the oldest transaction that was still pending when the snapshot
        // was taken.
        Map<String, Scn> snapshotPendingTransactions = offsetContext.getSnapshotPendingTransactions();
        if (snapshotPendingTransactions == null || snapshotPendingTransactions.isEmpty()) {
            // no pending transactions, we can start mining from the snapshot SCN
            startScn = snapshotScn;
        }
        else {
            // find the oldest transaction we can still fully process, and start from there.
            Scn minScn = snapshotScn;
            for (Map.Entry<String, Scn> entry : snapshotPendingTransactions.entrySet()) {
                String transactionId = entry.getKey();
                Scn scn = entry.getValue();
                LOGGER.info("Transaction {} was pending across snapshot boundary. Start SCN = {}, snapshot SCN = {}", transactionId, scn, startScn);
                if (scn.compareTo(firstScn) < 0) {
                    LOGGER.warn(
                            "Transaction {} was still ongoing while snapshot was taken, but is no longer completely recorded in the archive logs. Events will be lost. Oldest SCN in logs = {}, TX start SCN = {}",
                            transactionId, firstScn, scn);
                    minScn = firstScn;
                }
                else if (scn.compareTo(minScn) < 0) {
                    minScn = scn;
                }
            }

            // Make sure the commit SCN is at least the snapshot SCN - 1.
            // This ensures we'll never emit events for transactions that were complete before the snapshot was
            // taken.
            if (offsetContext.getCommitScn().compareTo(snapshotScn) < 0) {
                LOGGER.info("Setting commit SCN to {} (snapshot SCN - 1) to ensure we don't double-emit events from pre-snapshot transactions.",
                        snapshotScn.subtract(Scn.ONE));
                offsetContext.getCommitScn().setCommitScnOnAllThreads(snapshotScn.subtract(Scn.ONE));
            }

            // set start SCN to minScn
            if (minScn.compareTo(startScn) < 0) {
                LOGGER.info("Resetting start SCN from {} (snapshot SCN) to {} (start of oldest complete pending transaction)", startScn, minScn);
                startScn = minScn.subtract(Scn.ONE);
            }
        }
        offsetContext.setScn(startScn);
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
        final LogMiningBufferType bufferType = connectorConfig.getLogMiningBufferType();
        return bufferType.createProcessor(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, streamingMetrics);
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

    private void initializeRedoLogsForMining(OracleConnection connection, boolean postEndMiningSession, Scn startScn)
            throws SQLException {
        if (!postEndMiningSession) {
            if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                buildDataDictionary(connection);
            }
            if (!isContinuousMining) {
                currentLogFiles = setLogFilesForMining(connection, startScn, archiveLogRetention, archiveLogOnlyMode,
                        archiveDestinationName, logFileQueryMaxRetries, initialDelay, maxDelay);
                currentRedoLogSequences = getCurrentLogFileSequences(currentLogFiles);
            }
        }
        else {
            if (!isContinuousMining) {
                if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    buildDataDictionary(connection);
                }
                currentLogFiles = setLogFilesForMining(connection, startScn, archiveLogRetention, archiveLogOnlyMode,
                        archiveDestinationName, logFileQueryMaxRetries, initialDelay, maxDelay);
                currentRedoLogSequences = getCurrentLogFileSequences(currentLogFiles);
            }
        }

        updateRedoLogMetrics();
    }

    /**
     * Get the current log file sequences from the supplied list of log files.
     *
     * @param logFiles list of log files
     * @return list of sequences for the logs that are marked "current" in the database.
     */
    private List<BigInteger> getCurrentLogFileSequences(List<LogFile> logFiles) {
        if (logFiles == null || logFiles.isEmpty()) {
            return Collections.emptyList();
        }
        return logFiles.stream().filter(LogFile::isCurrent).map(LogFile::getSequence).collect(Collectors.toList());
    }

    /**
     * Get the maximum archive log SCN
     *
     * @param logFiles the current logs that are part of the mining session
     * @return the maximum system change number from the archive logs
     * @throws DebeziumException if no logs are provided or if the provided logs has no archive log types
     */
    private Scn getMaxArchiveLogScn(List<LogFile> logFiles) {
        if (logFiles == null || logFiles.isEmpty()) {
            throw new DebeziumException("Cannot get maximum archive log SCN as no logs were available.");
        }

        final List<LogFile> archiveLogs = logFiles.stream()
                .filter(log -> log.getType().equals(LogFile.Type.ARCHIVE))
                .collect(Collectors.toList());

        if (archiveLogs.isEmpty()) {
            throw new DebeziumException("Cannot get maximum archive log SCN as no archive logs are present.");
        }

        Scn maxScn = archiveLogs.get(0).getNextScn();
        for (int i = 1; i < archiveLogs.size(); ++i) {
            Scn nextScn = archiveLogs.get(i).getNextScn();
            if (nextScn.compareTo(maxScn) > 0) {
                maxScn = nextScn;
            }
        }

        LOGGER.debug("Maximum archive log SCN resolved as {}", maxScn);
        return maxScn;
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
     * @param startScn mining session's starting system change number (exclusive), should not be {@code null}
     * @param endScn mining session's ending system change number (inclusive), can be {@code null}
     * @param attempts the number of mining start attempts
     * @return true if the session was started successfully, false if it should be retried
     * @throws SQLException if mining session failed to start
     */
    public boolean startMiningSession(OracleConnection connection, Scn startScn, Scn endScn, int attempts) throws SQLException {
        LOGGER.debug("Starting mining session startScn={}, endScn={}, strategy={}, continuous={}",
                startScn, endScn, strategy, isContinuousMining);
        try {
            Instant start = Instant.now();
            // NOTE: we treat startSCN as the _exclusive_ lower bound for mining,
            // whereas START_LOGMNR takes an _inclusive_ lower bound, hence the increment.
            connection.executeWithoutCommitting(SqlUtils.startLogMinerStatement(startScn.add(Scn.ONE), endScn, strategy, isContinuousMining));
            streamingMetrics.addCurrentMiningSessionStart(Duration.between(start, Instant.now()));
            return true;
        }
        catch (SQLException e) {
            LogMinerDatabaseStateWriter.writeLogMinerStartParameters(connection);
            if (e.getErrorCode() == 1291 || e.getMessage().startsWith("ORA-01291")) {
                if (attempts <= MINING_START_RETRIES) {
                    LOGGER.warn("Failed to start Oracle LogMiner session, retrying...");
                    return false;
                }
                LOGGER.error("Failed to start Oracle LogMiner after '{}' attempts.", MINING_START_RETRIES, e);
                LogMinerDatabaseStateWriter.writeLogMinerLogFailures(connection);
            }
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
                ? getMaxArchiveLogScn(currentLogFiles)
                : connection.getCurrentScn();
        streamingMetrics.setCurrentScn(currentScn);

        // Add the current batch size to the starting system change number
        final Scn currentBatchSizeScn = Scn.valueOf(streamingMetrics.getBatchSize());
        Scn topScnToMine = startScn.add(currentBatchSizeScn);

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
                    Optional<Instant> prevEndScnTimestamp = connection.getScnToTimestamp(prevEndScn);
                    if (prevEndScnTimestamp.isPresent()) {
                        Optional<Instant> currentScnTimestamp = connection.getScnToTimestamp(currentScn);
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
     * Calculates the deviated end scn based on the scn range and deviation.
     *
     * @param lowerboundsScn the mining range's lower bounds
     * @param upperboundsScn the mining range's upper bounds
     * @param deviation the time deviation
     * @return an optional that contains the deviated scn or empty if the operation should be performed again
     */
    private Optional<Scn> calculateDeviatedEndScn(Scn lowerboundsScn, Scn upperboundsScn, Duration deviation) {
        if (archiveLogOnlyMode) {
            // When archive-only mode is enabled, deviation should be ignored, even when enabled.
            return Optional.of(upperboundsScn);
        }

        final Optional<Scn> calculatedDeviatedEndScn = getDeviatedMaxScn(upperboundsScn, deviation);
        if (calculatedDeviatedEndScn.isEmpty() || calculatedDeviatedEndScn.get().isNull()) {
            // This happens only if the deviation calculation is outside the flashback/undo area or an exception was thrown.
            // In this case we have no choice but to use the upper bounds as a fallback.
            LOGGER.warn("Mining session end SCN deviation calculation is outside undo space, using upperbounds {}. If this continues, " +
                    "consider lowering the value of the '{}' configuration property.", upperboundsScn,
                    OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS.name());
            return Optional.of(upperboundsScn);
        }
        else if (calculatedDeviatedEndScn.get().compareTo(lowerboundsScn) <= 0) {
            // This should also force the outer loop to recall this method again.
            LOGGER.debug("Mining session end SCN deviation as {}, outside of mining range, recalculating.", calculatedDeviatedEndScn.get());
            return Optional.empty();
        }
        else {
            // Calculated SCN is after lower bounds and within flashback/undo area, safe to return.
            return calculatedDeviatedEndScn;
        }
    }

    /**
     * Uses the provided Upperbound SCN and deviation to calculate an SCN that happened in the past at a
     * time based on Oracle's {@code TIMESTAMP_TO_SCN} and {@code SCN_TO_TIMESTAMP} functions.
     *
     * @param upperboundsScn the upper bound system change number, should not be {@code null}
     * @param deviation the time deviation to be applied, should not be {@code null}
     * @return the newly calculated Scn
     */
    private Optional<Scn> getDeviatedMaxScn(Scn upperboundsScn, Duration deviation) {
        try {
            final Scn currentScn = jdbcConnection.getCurrentScn();
            final Optional<Instant> currentInstant = jdbcConnection.getScnToTimestamp(currentScn);
            final Optional<Instant> upperInstant = jdbcConnection.getScnToTimestamp(upperboundsScn);
            if (currentInstant.isPresent() && upperInstant.isPresent()) {
                // If the upper bounds satisfies the deviation time
                if (Duration.between(upperInstant.get(), currentInstant.get()).compareTo(deviation) >= 0) {
                    LOGGER.trace("Upper bounds {} is within deviation period, using it.", upperboundsScn);
                    return Optional.of(upperboundsScn);
                }
            }
            return Optional.of(jdbcConnection.getScnAdjustedByTime(upperboundsScn, deviation));
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to calculate deviated max SCN value from {}.", upperboundsScn);
            return Optional.empty();
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
                    if (!connection.isTableExists(tableId)) {
                        LOGGER.warn("Database table '{}' no longer exists, supplemental log check skipped", tableId);
                    }
                    else if (!isTableAllColumnsSupplementalLoggingEnabled(connection, tableId)) {
                        LOGGER.warn("Database table '{}' not configured with supplemental logging \"(ALL) COLUMNS\"; " +
                                "only explicitly changed columns will be captured. " +
                                "Use: ALTER TABLE {}.{} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableId, tableId.schema(), tableId.table());
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
        if (connectorConfig.isLogMiningReadOnly()) {
            return new ReadOnlyLogWriterFlushStrategy();
        }
        if (connectorConfig.isRacSystem()) {
            return new RacCommitLogWriterFlushStrategy(connectorConfig, jdbcConfiguration, streamingMetrics);
        }
        return new CommitLogWriterFlushStrategy(connectorConfig, jdbcConnection);
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
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        // nothing to do
    }

    @Override
    public OracleOffsetContext getOffsetContext() {
        return effectiveOffset;
    }
}
