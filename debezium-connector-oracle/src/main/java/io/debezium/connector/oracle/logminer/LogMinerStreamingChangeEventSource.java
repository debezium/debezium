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
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getTimeDifference;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.instantiateFlushConnections;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.logError;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setNlsSessionParameters;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setRedoLogFilesForMining;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.startLogMining;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
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
import io.debezium.connector.oracle.OracleTaskContext;
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
    private final String catalogName;
    private final boolean isRac;
    private final Set<String> racHosts = new HashSet<>();
    private final JdbcConfiguration jdbcConfiguration;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final OracleTaskContext taskContext;
    private final ErrorHandler errorHandler;
    private final boolean isContinuousMining;

    private OracleConnectorConfig connectorConfig;
    private LogMinerMetrics logMinerMetrics;
    private long startScn;
    private long endScn;
    private Duration archiveLogRetention;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext,
                                              OracleConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                              OracleTaskContext taskContext, Configuration jdbcConfig) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.connectorConfig = connectorConfig;
        this.catalogName = (connectorConfig.getPdbName() != null) ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.isRac = connectorConfig.isRacSystem();
        if (this.isRac) {
            this.racHosts.addAll(connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet()));
            instantiateFlushConnections(jdbcConfiguration, racHosts);
        }
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context
     *         change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context) {
        try (TransactionalBuffer transactionalBuffer = new TransactionalBuffer(taskContext, errorHandler)) {
            try {
                // Perform registration
                registerLogMinerMetrics();

                long databaseTimeMs = getTimeDifference(jdbcConnection).toMillis();
                LOGGER.trace("Current time {} ms, database difference {} ms", System.currentTimeMillis(), databaseTimeMs);
                transactionalBuffer.setDatabaseTimeDifference(databaseTimeMs);

                startScn = offsetContext.getScn();
                createFlushTable(jdbcConnection);

                if (!isContinuousMining && startScn < getFirstOnlineLogScn(jdbcConnection, archiveLogRetention)) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters(jdbcConnection);
                checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

                initializeRedoLogsForMining(jdbcConnection, false, archiveLogRetention);

                HistoryRecorder historyRecorder = connectorConfig.getLogMiningHistoryRecorder();

                Duration processTime = Duration.ZERO;
                try {
                    // todo: why can't OracleConnection be used rather than a Factory+JdbcConfiguration?
                    historyRecorder.prepare(logMinerMetrics, jdbcConfiguration, connectorConfig.getLogMinerHistoryRetentionHours());

                    final LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context, jdbcConnection,
                            connectorConfig, logMinerMetrics, transactionalBuffer, offsetContext, schema, dispatcher,
                            catalogName, clock, historyRecorder);

                    final String query = SqlUtils.logMinerContentsQuery(connectorConfig.getSchemaName(), jdbcConnection.username(), schema);
                    try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                        Set<String> currentRedoLogFiles = getCurrentRedoLogFiles(jdbcConnection, logMinerMetrics);

                        Stopwatch stopwatch = Stopwatch.reusable();
                        while (context.isRunning()) {
                            Instant start = Instant.now();
                            endScn = getEndScn(jdbcConnection, startScn, logMinerMetrics, connectorConfig.getLogMiningBatchSizeDefault());
                            flushLogWriter(jdbcConnection, jdbcConfiguration, isRac, racHosts);

                            Set<String> possibleNewCurrentLogFile = getCurrentRedoLogFiles(jdbcConnection, logMinerMetrics);
                            if (!currentRedoLogFiles.equals(possibleNewCurrentLogFile)) {
                                LOGGER.debug("Redo log switch detected, from {} to {}", currentRedoLogFiles, possibleNewCurrentLogFile);

                                // This is the way to mitigate PGA leaks.
                                // With one mining session, it grows and maybe there is another way to flush PGA.
                                // At this point we use a new mining session
                                LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                        startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                                endMining(jdbcConnection);

                                initializeRedoLogsForMining(jdbcConnection, true, archiveLogRetention);

                                abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);
                                currentRedoLogFiles = getCurrentRedoLogFiles(jdbcConnection, logMinerMetrics);
                            }

                            startLogMining(jdbcConnection, startScn, endScn, strategy, isContinuousMining, logMinerMetrics);

                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setLong(1, startScn);
                            miningView.setLong(2, endScn);
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing = stopwatch.stop().durations().statistics().getTotal();
                                logMinerMetrics.setLastDurationOfBatchCapturing(lastDurationOfBatchCapturing);
                                processor.processResult(rs);

                                startScn = endScn;

                                if (transactionalBuffer.isEmpty()) {
                                    LOGGER.debug("Transactional buffer empty, updating offset's SCN {}", startScn);
                                    offsetContext.setScn(startScn);
                                }
                            }

                            processTime = processTime.plus(Duration.between(start, Instant.now()));
                            logMinerMetrics.setTotalProcessingTime(processTime);

                            pauseBetweenMiningSessions();
                        }
                    }
                }
                finally {
                    historyRecorder.close();
                }
            }
            catch (Throwable t) {
                logError(transactionalBuffer.getMetrics(), "Mining session stopped due to the {}", t);
                errorHandler.setProducerThrowable(t);
            }
            finally {
                LOGGER.info("startScn={}, endScn={}, offsetContext.getScn()={}", startScn, endScn, offsetContext.getScn());
                LOGGER.info("Transactional buffer metrics dump: {}", transactionalBuffer.getMetrics().toString());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.getMetrics().toString());
                LOGGER.info("LogMiner metrics dump: {}", logMinerMetrics.toString());

                // Perform unregistration
                unregisterLogMinerMetrics();
            }
        }
    }

    private void registerLogMinerMetrics() {
        logMinerMetrics = new LogMinerMetrics(taskContext, connectorConfig);
        logMinerMetrics.register(LOGGER);
        if (connectorConfig.isLogMiningHistoryRecorded()) {
            logMinerMetrics.setRecordMiningHistory(true);
        }
    }

    private void unregisterLogMinerMetrics() {
        if (logMinerMetrics != null) {
            logMinerMetrics.unregister(LOGGER);
        }
    }

    private void abandonOldTransactionsIfExist(OracleConnection connection, TransactionalBuffer transactionalBuffer) {
        Optional<Long> lastScnToAbandonTransactions = getLastScnToAbandon(connection, offsetContext.getScn(), connectorConfig.getLogMiningTransactionRetention());
        lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
            transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
            offsetContext.setScn(thresholdScn);
            startScn = endScn;
        });
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

    private void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period = Duration.ofMillis(logMinerMetrics.getMillisecondToSleepBetweenMiningQuery());
        Metronome.sleeper(period, clock).pause();
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
