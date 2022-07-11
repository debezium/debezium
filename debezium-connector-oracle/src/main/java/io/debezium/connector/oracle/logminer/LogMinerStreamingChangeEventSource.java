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
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setNlsSessionParameters;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setRedoLogFilesForMining;
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
import java.util.Objects;
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
import io.debezium.connector.oracle.OracleTaskContext;
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
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean isRac;
    private final Set<String> racHosts = new HashSet<>();
    private final JdbcConfiguration jdbcConfiguration;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final OracleTaskContext taskContext;
    private final ErrorHandler errorHandler;
    private final boolean isContinuousMining;
    private final boolean committedDataOnly;
    private final String extOptions;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final OracleConnectorConfig connectorConfig;
    private final Duration archiveLogRetention;

    private Scn initScn;

    private Scn startScn;
    private Scn endScn;
    private List<BigInteger> currentRedoLogSequences;

    private final String bigTransactionalCachePath;

    private final int bigTransactionalLimitCount;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext,
                                              OracleConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                              OracleTaskContext taskContext, Configuration jdbcConfig,
                                              OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.connectorConfig = connectorConfig;
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.committedDataOnly = connectorConfig.isCommittedDataOnly();
        this.extOptions = connectorConfig.getExtOptions();
        this.initScn = connectorConfig.getInitScn() == 0 ? null : new Scn(BigInteger.valueOf(connectorConfig.getInitScn()));
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.isRac = connectorConfig.isRacSystem();
        if (this.isRac) {
            this.racHosts.addAll(connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet()));
            instantiateFlushConnections(jdbcConfiguration, racHosts);
        }
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
        this.bigTransactionalCachePath = connectorConfig.getBigTransactionalCachePath();
        this.bigTransactionalLimitCount = connectorConfig.getBigTransactionalLimitCount();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context
     *         change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context) {
        try (TransactionalBuffer transactionalBuffer = new TransactionalBuffer(schema, clock, errorHandler, streamingMetrics, bigTransactionalCachePath,
                bigTransactionalLimitCount)) {
            try {
                startScn = Objects.isNull(initScn) ? offsetContext.getScn() : initScn;
                createFlushTable(jdbcConnection);

                if (!isContinuousMining && startScn.compareTo(getFirstOnlineLogScn(jdbcConnection, archiveLogRetention)) < 0) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters(jdbcConnection);
                checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

                initializeRedoLogsForMining(jdbcConnection, false, archiveLogRetention);

                HistoryRecorder historyRecorder = connectorConfig.getLogMiningHistoryRecorder();

                try {
                    // todo: why can't OracleConnection be used rather than a Factory+JdbcConfiguration?
                    historyRecorder.prepare(streamingMetrics, jdbcConfiguration, connectorConfig.getLogMinerHistoryRetentionHours());

                    final LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context, jdbcConnection,
                            connectorConfig, streamingMetrics, transactionalBuffer, offsetContext, schema, dispatcher,
                            clock, historyRecorder);

                    final String query = SqlUtils.logMinerContentsQuery(connectorConfig, jdbcConnection.username());
                    try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {

                        currentRedoLogSequences = getCurrentRedoLogSequences();
                        Stopwatch stopwatch = Stopwatch.reusable();
                        while (context.isRunning()) {
                            // Calculate time difference before each mining session to detect time zone offset changes (e.g. DST) on database server
                            streamingMetrics.calculateTimeDifference(getSystime(jdbcConnection));

                            Instant start = Instant.now();
                            endScn = getEndScn(jdbcConnection, startScn, streamingMetrics, connectorConfig.getLogMiningBatchSizeDefault());
                            flushLogWriter(jdbcConnection, jdbcConfiguration, isRac, racHosts);

                            if (hasLogSwitchOccurred()) {
                                // This is the way to mitigate PGA leaks.
                                // With one mining session, it grows and maybe there is another way to flush PGA.
                                // At this point we use a new mining session
                                LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                        startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                                endMining(jdbcConnection);

                                initializeRedoLogsForMining(jdbcConnection, true, archiveLogRetention);

                                abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);

                                // This needs to be re-calculated because building the data dictionary will force the
                                // current redo log sequence to be advanced due to a complete log switch of all logs.
                                currentRedoLogSequences = getCurrentRedoLogSequences();
                            }

                            startLogMining(jdbcConnection, startScn, endScn, strategy, isContinuousMining, streamingMetrics, committedDataOnly, extOptions);

                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setString(1, startScn.toString());
                            miningView.setString(2, endScn.toString());
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing = stopwatch.stop().durations().statistics().getTotal();
                                streamingMetrics.setLastDurationOfBatchCapturing(lastDurationOfBatchCapturing);
                                processor.processResult(rs);

                                startScn = endScn;

                                if (transactionalBuffer.isEmpty()) {
                                    LOGGER.debug("Transactional buffer empty, updating offset's SCN {}", startScn);
                                    offsetContext.setScn(startScn);
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

    private void abandonOldTransactionsIfExist(OracleConnection connection, TransactionalBuffer transactionalBuffer) {
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

    private void initializeRedoLogsForMining(OracleConnection connection, boolean postEndMiningSession, Duration archiveLogRetention) throws SQLException {
        if (!postEndMiningSession) {
            if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                buildDataDictionary(connection);
            }
            if (!isContinuousMining) {
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        }
        else {
            if (!isContinuousMining) {
                if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    buildDataDictionary(connection);
                }
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
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

            final int logSwitchCount = jdbcConnection.queryAndMap(SqlUtils.switchHistoryQuery(), rs -> {
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

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
