/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.buildDataDictionary;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.checkSupplementalLogging;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.createFlushTable;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.endMining;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.flushLogWriter;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getCurrentRedoLogFiles;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getEndScn;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getFirstOnlineLogScn;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getLastScnToAbandon;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getSystime;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.instantiateFlushConnections;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.logError;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setLogFilesForMining;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setNlsSessionParameters;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.startLogMining;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource<OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final boolean isRac;
    private final Set<String> racHosts = new HashSet<>();
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
        this.isRac = connectorConfig.isRacSystem();
        if (this.isRac) {
            this.racHosts.addAll(connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet()));
            instantiateFlushConnections(jdbcConfiguration, racHosts);
        }
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
    public void execute(ChangeEventSourceContext context, OracleOffsetContext offsetContext) {
        try (TransactionalBuffer transactionalBuffer = new TransactionalBuffer(connectorConfig, schema, clock, errorHandler, streamingMetrics)) {
            try {
                startScn = offsetContext.getScn();
                createFlushTable(jdbcConnection);

                if (!isContinuousMining && startScn.compareTo(getFirstOnlineLogScn(jdbcConnection, archiveLogRetention, archiveDestinationName)) < 0) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters(jdbcConnection);
                checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

                if (archiveLogOnlyMode && !waitForStartScnInArchiveLogs(context, startScn)) {
                    return;
                }

                initializeRedoLogsForMining(jdbcConnection, false, startScn);

                HistoryRecorder historyRecorder = connectorConfig.getLogMiningHistoryRecorder();

                try {
                    // todo: why can't OracleConnection be used rather than a Factory+JdbcConfiguration?
                    historyRecorder.prepare(streamingMetrics, jdbcConfiguration, connectorConfig.getLogMinerHistoryRetentionHours());

                    final LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context,
                            connectorConfig, streamingMetrics, transactionalBuffer, offsetContext, schema, dispatcher,
                            historyRecorder);

                    final String query = LogMinerQueryBuilder.build(connectorConfig, schema, jdbcConnection.username());
                    try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {

                        currentRedoLogSequences = getCurrentRedoLogSequences();
                        Stopwatch stopwatch = Stopwatch.reusable();
                        while (context.isRunning()) {
                            // Calculate time difference before each mining session to detect time zone offset changes (e.g. DST) on database server
                            streamingMetrics.calculateTimeDifference(getSystime(jdbcConnection));

                            if (archiveLogOnlyMode && !waitForStartScnInArchiveLogs(context, startScn)) {
                                break;
                            }

                            Instant start = Instant.now();
                            endScn = getEndScn(jdbcConnection, startScn, endScn, streamingMetrics, connectorConfig.getLogMiningBatchSizeDefault(),
                                    connectorConfig.isLobEnabled(), connectorConfig.isArchiveLogOnlyMode(), connectorConfig.getLogMiningArchiveDestinationName());

                            // This is a small window where when archive log only mode has completely caught up to the last
                            // record in the archive logs that both the start and end values are identical. In this use
                            // case we want to pause and restart the loop waiting for a new archive log before proceeding.
                            if (archiveLogOnlyMode && startScn.equals(endScn)) {
                                pauseBetweenMiningSessions();
                                continue;
                            }

                            flushLogWriter(jdbcConnection, jdbcConfiguration, isRac, racHosts);

                            if (hasLogSwitchOccurred()) {
                                // This is the way to mitigate PGA leaks.
                                // With one mining session, it grows and maybe there is another way to flush PGA.
                                // At this point we use a new mining session
                                LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                        startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                                endMining(jdbcConnection);

                                initializeRedoLogsForMining(jdbcConnection, true, startScn);

                                abandonOldTransactionsIfExist(jdbcConnection, offsetContext, transactionalBuffer);

                                // This needs to be re-calculated because building the data dictionary will force the
                                // current redo log sequence to be advanced due to a complete log switch of all logs.
                                currentRedoLogSequences = getCurrentRedoLogSequences();
                            }

                            startLogMining(jdbcConnection, startScn, endScn, strategy, isContinuousMining, streamingMetrics);

                            LOGGER.trace("Fetching LogMiner view results SCN {} to {}", startScn, endScn);
                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setString(1, startScn.toString());
                            miningView.setString(2, endScn.toString());
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing = stopwatch.stop().durations().statistics().getTotal();
                                streamingMetrics.setLastDurationOfBatchCapturing(lastDurationOfBatchCapturing);
                                processor.processResult(rs);
                                if (connectorConfig.isLobEnabled()) {
                                    startScn = transactionalBuffer.updateOffsetContext(offsetContext, dispatcher);
                                }
                                else {

                                    final Scn lastProcessedScn = processor.getLastProcessedScn();
                                    if (!lastProcessedScn.isNull() && lastProcessedScn.compareTo(endScn) < 0) {
                                        // If the last processed SCN is before the endScn we need to use the last processed SCN as the
                                        // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
                                        endScn = lastProcessedScn;
                                    }

                                    if (transactionalBuffer.isEmpty()) {
                                        LOGGER.debug("Buffer is empty, updating offset SCN to {}", endScn);
                                        offsetContext.setScn(endScn);
                                    }
                                    else {
                                        final Scn minStartScn = transactionalBuffer.getMinimumScn();
                                        if (!minStartScn.isNull()) {
                                            offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                                            dispatcher.dispatchHeartbeatEvent(offsetContext);
                                        }
                                    }
                                    startScn = endScn;
                                }
                            }

                            streamingMetrics.setCurrentBatchProcessingTime(Duration.between(start, Instant.now()));
                            pauseBetweenMiningSessions();
                        }
                    }
                }
                finally {
                    historyRecorder.close();
                }
            }
            catch (Throwable t) {
                logError(streamingMetrics, "Mining session stopped due to the {}", t);
                errorHandler.setProducerThrowable(t);
            }
            finally {
                LOGGER.info("startScn={}, endScn={}, offsetContext.getScn()={}", startScn, endScn, offsetContext.getScn());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.toString());
                LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            }
        }
    }

    private void abandonOldTransactionsIfExist(OracleConnection connection, OracleOffsetContext offsetContext, TransactionalBuffer transactionalBuffer) {
        Duration transactionRetention = connectorConfig.getLogMiningTransactionRetention();
        if (!Duration.ZERO.equals(transactionRetention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(connection, offsetScn, transactionRetention);
            lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
                transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
                offsetContext.setScn(thresholdScn);
                startScn = endScn;
            });
        }
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

            final Map<String, String> logStatuses = jdbcConnection.queryAndMap(SqlUtils.redoLogStatusQuery(), rs -> {
                Map<String, String> results = new LinkedHashMap<>();
                while (rs.next()) {
                    results.put(rs.getString(1), rs.getString(2));
                }
                return results;
            });

            final int logSwitchCount = jdbcConnection.queryAndMap(SqlUtils.switchHistoryQuery(archiveDestinationName), rs -> {
                if (rs.next()) {
                    return rs.getInt(2);
                }
                return 0;
            });

            final Set<String> fileNames = getCurrentRedoLogFiles(jdbcConnection);

            streamingMetrics.setRedoLogStatus(logStatuses);
            streamingMetrics.setSwitchCount(logSwitchCount);
            streamingMetrics.setCurrentLogFileName(fileNames);

            return true;
        }

        return false;
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
