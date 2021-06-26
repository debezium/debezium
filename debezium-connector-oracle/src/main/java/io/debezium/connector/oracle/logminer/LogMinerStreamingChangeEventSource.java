/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.checkSupplementalLogging;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getCurrentRedoLogFiles;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.getEndScn;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.logError;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setLogFilesForMining;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
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
    private Map<String, OracleConnection> flushConnections;

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
        if (connectorConfig.isRacSystem()) {
            this.racHosts.addAll(connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet()));
            instantiateRacFlushConnections(jdbcConfiguration, racHosts);
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
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext) {
        try (TransactionalBuffer transactionalBuffer = new TransactionalBuffer(connectorConfig, schema, clock, errorHandler, streamingMetrics)) {
            try {
                startScn = offsetContext.getScn();
                createFlushTable(jdbcConnection);

                if (!isContinuousMining && startScn.compareTo(getFirstScnInLogs(jdbcConnection)) < 0) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters(jdbcConnection);
                checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

                initializeRedoLogsForMining(jdbcConnection, false, startScn);

                final LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context,
                        connectorConfig, streamingMetrics, transactionalBuffer, offsetContext, schema, dispatcher);

                final String query = LogMinerQueryBuilder.build(connectorConfig);
                try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {

                    currentRedoLogSequences = getCurrentRedoLogSequences();
                    Stopwatch stopwatch = Stopwatch.reusable();
                    while (context.isRunning()) {
                        // Calculate time difference before each mining session to detect time zone offset changes (e.g. DST) on database server
                        streamingMetrics.calculateTimeDifference(getDatabaseSystemTime(jdbcConnection));

                        Instant start = Instant.now();
                        endScn = getEndScn(jdbcConnection, startScn, endScn, streamingMetrics, connectorConfig.getLogMiningBatchSizeDefault(),
                                connectorConfig.isLobEnabled());
                        flushLogWriter(jdbcConnection, jdbcConfiguration, racHosts);

                        if (hasLogSwitchOccurred()) {
                            // This is the way to mitigate PGA leaks.
                            // With one mining session, it grows and maybe there is another way to flush PGA.
                            // At this point we use a new mining session
                            LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                    startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                            endMiningSession(jdbcConnection);

                            initializeRedoLogsForMining(jdbcConnection, true, startScn);

                            abandonOldTransactionsIfExist(jdbcConnection, offsetContext, transactionalBuffer);

                            // This needs to be re-calculated because building the data dictionary will force the
                            // current redo log sequence to be advanced due to a complete log switch of all logs.
                            currentRedoLogSequences = getCurrentRedoLogSequences();
                        }

                        startMiningSession(jdbcConnection, startScn, endScn);

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
                                if (transactionalBuffer.isEmpty()) {
                                    LOGGER.debug("Buffer is empty, updating offset SCN to {}", endScn);
                                    offsetContext.setScn(endScn);
                                }
                                startScn = endScn;
                            }
                        }

                        captureSessionMemoryStatistics(jdbcConnection);

                        streamingMetrics.setCurrentBatchProcessingTime(Duration.between(start, Instant.now()));
                        pauseBetweenMiningSessions();
                    }
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

    private void captureSessionMemoryStatistics(OracleConnection connection) throws SQLException {
        long sessionUserGlobalAreaMemory = connection.getSessionStatisticByName("session uga memory");
        long sessionUserGlobalAreaMaxMemory = connection.getSessionStatisticByName("session uga memory max");
        streamingMetrics.setUserGlobalAreaMemory(sessionUserGlobalAreaMemory, sessionUserGlobalAreaMaxMemory);

        long sessionProcessGlobalAreaMemory = connection.getSessionStatisticByName("session pga memory");
        long sessionProcessGlobalAreaMaxMemory = connection.getSessionStatisticByName("session pga memory max");
        streamingMetrics.setProcessGlobalAreaMemory(sessionProcessGlobalAreaMemory, sessionProcessGlobalAreaMaxMemory);

        final DecimalFormat format = new DecimalFormat("#.##");
        LOGGER.info("Oracle Session UGA {}MB (max = {}MB), PGA {}MB (max = {}MB)",
                format.format(sessionUserGlobalAreaMemory / 1024.f / 1024.f),
                format.format(sessionUserGlobalAreaMaxMemory / 1024.f / 1024.f),
                format.format(sessionProcessGlobalAreaMemory / 1024.f / 1024.f),
                format.format(sessionProcessGlobalAreaMaxMemory / 1024.f / 1024.f));
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

    /**
     * Calculates the SCN as a watermark to abandon for long running transactions.
     * The criteria is do not let the offset SCN expire from archives older the specified retention hours.
     *
     * @param connection database connection, should not be {@code null}
     * @param offsetScn offset system change number, should not be {@code null}
     * @param retention duration to tolerate long running transactions before being abandoned, must not be {@code null}
     * @return an optional system change number as the watermark for transaction buffer abandonment
     */
    private Optional<Scn> getLastScnToAbandon(OracleConnection connection, Scn offsetScn, Duration retention) {
        try {
            Float diffInDays = connection.singleOptionalValue(SqlUtils.diffInDaysQuery(offsetScn), rs -> rs.getFloat(1));
            if (diffInDays != null && (diffInDays * 24) > retention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference for transaction abandonment", e);
            streamingMetrics.incrementErrorCount();
            return Optional.of(offsetScn);
        }
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
        connection.executeWithoutCommitting(SqlUtils.BUILD_DICTIONARY);
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
     * Flushes the Oracle LGWR buffer if connected to a standalone Oracle instance or all LGWR buffers on
     * each Oracle RAC node when connected to an Oracle RAC cluster.
     *
     * @param connection database connection the primary instance, must not be {@code null}
     * @param jdbcConfig database configuration, must not be {@code null}
     * @param hosts set of RAC hosts or ip addresses, should not be {@code null} but may be empty.
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the flushing to an Oracle RAC cluster was interrupted
     */
    private void flushLogWriter(OracleConnection connection, JdbcConfiguration jdbcConfig, Set<String> hosts) throws SQLException, InterruptedException {
        Scn currentScn = connection.getCurrentScn();
        if (!hosts.isEmpty()) {
            flushRacLogWriters(currentScn, jdbcConfig, hosts);
        }
        else {
            LOGGER.trace("Flushing LGWR buffer on instance '{}'", jdbcConfig.getHostname());
            connection.executeWithoutCommitting(SqlUtils.UPDATE_FLUSH_TABLE + currentScn);
            connection.commit();
        }
    }

    /**
     * An Oracle RAC cluster has one LGWR process per node and each needs to be flushed.
     * Queries to {@code GV$INSTANCE} cannot be used because not all nodes may be load balanced by the cluster.
     *
     * @param currentScn value to be flushed to the node's local flush table, must not be {@code null}
     * @param jdbcConfig database configuration, must not be {@code null}
     * @param hosts set of RAC hosts or ip addresses, should not be {@code null} or empty.
     * @throws InterruptedException if the flushing to the Oracle RAC cluster was interrupted
     */
    private void flushRacLogWriters(Scn currentScn, JdbcConfiguration jdbcConfig, Set<String> hosts) throws InterruptedException {
        Instant startTime = Instant.now();

        boolean recreateConnections = false;
        for (String hostName : hosts) {
            try {
                final OracleConnection connection = flushConnections.get(hostName);
                if (connection == null) {
                    LOGGER.warn("Connection to RAC node '{}' does not exist; will be re-created.", hostName);
                    recreateConnections = true;
                    continue;
                }
                LOGGER.trace("Flushing LGWR buffer on RAC node '{}'", hostName);
                connection.executeWithoutCommitting(SqlUtils.UPDATE_FLUSH_TABLE + currentScn);
                connection.commit();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to flush LGWR buffer on RAC node '{}'", hostName, e);
                recreateConnections = true;
            }
        }

        if (recreateConnections) {
            instantiateRacFlushConnections(jdbcConfig, hosts);
            LOGGER.warn("Not all LGWR buffers were flushed, waiting 3 seconds for Oracle to flush automatically.");
            Metronome pause = Metronome.sleeper(Duration.ofSeconds(3), Clock.system());
            try {
                pause.pause();
            }
            catch (InterruptedException e) {
                LOGGER.warn("The LGWR buffer wait was interrupted.");
                throw e;
            }
        }

        LOGGER.trace("LGWR flush took {} to complete.", Duration.between(startTime, Instant.now()));
    }

    /**
     * Instantiates a RAC flush connection to each RAC cluster node.
     *
     * @param jdbcConfig database connection configuration
     * @param hosts set of Oracle RAC node hosts or ip addresses
     */
    private void instantiateRacFlushConnections(JdbcConfiguration jdbcConfig, Set<String> hosts) {
        if (flushConnections == null) {
            flushConnections = new HashMap<>();
        }

        // If any existing connections, close them.
        for (Map.Entry<String, OracleConnection> entry : flushConnections.entrySet()) {
            final String hostName = entry.getKey();
            final OracleConnection connection = entry.getValue();
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (SQLException e) {
                    // It's fine not to throw this exception, a new connection will be established
                    LOGGER.warn("Failed to close RAC connection to node '{}'", hostName, e);
                    streamingMetrics.incrementWarningCount();
                }
            }
        }

        flushConnections.clear();

        // Create new connections
        final Supplier<ClassLoader> classLoaderSupplier = LogMinerStreamingChangeEventSource.class::getClassLoader;
        for (String hostName : hosts) {
            try {
                JdbcConfiguration jdbcHostConfig = JdbcConfiguration.adapt(jdbcConfig.edit()
                        .with(JdbcConfiguration.HOSTNAME, hostName).build());
                LOGGER.debug("Creating flush connection to RAC node '{}'", hostName);
                final OracleConnection flushConnection = new OracleConnection(jdbcHostConfig, classLoaderSupplier);
                flushConnection.setAutoCommit(false);

                flushConnections.put(hostName, flushConnection);
            }
            catch (SQLException e) {
                throw new DebeziumException("Cannot connect to RAC node '" + hostName + "'", e);
            }
        }
    }

    /**
     * Creates the flush table used to force flushing of the Oracle LGWR buffers.
     *
     * @param connection database connection
     * @throws SQLException if a database exception occurred
     */
    private void createFlushTable(OracleConnection connection) throws SQLException {
        String tableExists = connection.singleOptionalValue(SqlUtils.tableExistsQuery(SqlUtils.LOGMNR_FLUSH_TABLE), rs -> rs.getString(1));
        if (tableExists == null) {
            connection.executeWithoutCommitting(SqlUtils.CREATE_FLUSH_TABLE);
        }

        String recordExists = connection.singleOptionalValue(SqlUtils.FLUSH_TABLE_NOT_EMPTY, rs -> rs.getString(1));
        if (recordExists == null) {
            connection.executeWithoutCommitting(SqlUtils.INSERT_FLUSH_TABLE);
            connection.commit();
        }
    }

    /**
     * Sets the NLS parameters for the mining session.
     *
     * @param connection database connection, should not be {@code null}
     * @throws SQLException if a database exception occurred
     */
    private void setNlsSessionParameters(OracleConnection connection) throws SQLException {
        connection.executeWithoutCommitting(SqlUtils.NLS_SESSION_PARAMETERS);
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
        return connection.singleOptionalValue(SqlUtils.SELECT_SYSTIMESTAMP, rs -> rs.getObject(1, OffsetDateTime.class));
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
            // Capture the database state before throwing the exception up
            LogMinerHelper.logDatabaseState(connection);
            throw e;
        }
    }

    /**
     * End the current Oracle LogMiner session, if one is in progress.  If the current session does not
     * have an active mining session, a log message is recorded and the method is a no-op.
     *
     * @param connection database connection, should not be {@code null}
     * @throws SQLException if the current mining session cannot be ended gracefully
     */
    public void endMiningSession(OracleConnection connection) throws SQLException {
        try {
            connection.executeWithoutCommitting(SqlUtils.END_LOGMNR);
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

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
