/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * This class contains methods to configure and manage LogMiner utility
 */
public class LogMinerHelper {

    private static final String CURRENT = "CURRENT";
    private static final String UNKNOWN = "unknown";
    private static final String TOTAL = "TOTAL";
    private static final String ALL_COLUMN_LOGGING = "ALL COLUMN LOGGING";
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    public enum DATATYPE {
        LONG,
        TIMESTAMP,
        STRING,
        FLOAT
    }

    private static Map<String, OracleConnection> racFlushConnections = new HashMap<>();

    static void instantiateFlushConnections(JdbcConfiguration config, Set<String> hosts) {
        for (OracleConnection conn : racFlushConnections.values()) {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException e) {
                    LOGGER.warn("Cannot close existing RAC flush connection", e);
                }
            }
        }
        racFlushConnections = new HashMap<>();
        for (String host : hosts) {
            try {
                racFlushConnections.put(host, createFlushConnection(config, host));
            }
            catch (SQLException e) {
                LOGGER.error("Cannot connect to RAC node {}", host, e);
            }
        }
    }

    /**
     * This builds data dictionary objects in redo log files.
     * During this build, Oracle does an additional REDO LOG switch.
     * This call may take time, which leads to delay in delivering incremental changes.
     * With this option the lag between source database and dispatching event fluctuates.
     *
     * @param connection connection to the database as LogMiner user (connection to the container)
     * @throws SQLException any exception
     */
    static void buildDataDictionary(OracleConnection connection) throws SQLException {
        LOGGER.trace("Building data dictionary");
        executeCallableStatement(connection, SqlUtils.BUILD_DICTIONARY);
    }

    static void createFlushTable(OracleConnection connection) throws SQLException {
        String tableExists = (String) getSingleResult(connection, SqlUtils.tableExistsQuery(SqlUtils.LOGMNR_FLUSH_TABLE), DATATYPE.STRING);
        if (tableExists == null) {
            executeCallableStatement(connection, SqlUtils.CREATE_FLUSH_TABLE);
        }

        String recordExists = (String) getSingleResult(connection, SqlUtils.FLUSH_TABLE_NOT_EMPTY, DATATYPE.STRING);
        if (recordExists == null) {
            executeCallableStatement(connection, SqlUtils.INSERT_FLUSH_TABLE);
            connection.commit();
        }
    }

    /**
     * This method returns next SCN for mining and also updates streaming metrics.
     *
     * We use a configurable limit, because the larger mining range, the slower query from LogMiner content view.
     * In addition capturing unlimited number of changes can blow up Java heap.
     * Gradual querying helps to catch up faster after long delays in mining.
     *
     * @param connection container level database connection
     * @param startScn start SCN
     * @param prevEndScn previous end SCN
     * @param streamingMetrics the streaming metrics
     * @param lobEnabled specifies whether LOB support is enabled
     * @param archiveLogOnlyMode specifies whether archive log only mode is enabled
     * @param archiveLogDestinationName the archive log destination name
     * @return next SCN to mine up to
     * @throws SQLException if anything unexpected happens
     */
    static Scn getEndScn(OracleConnection connection, Scn startScn, Scn prevEndScn, OracleStreamingChangeEventSourceMetrics streamingMetrics, int defaultBatchSize,
                         boolean lobEnabled, boolean archiveLogOnlyMode, String archiveLogDestinationName)
            throws SQLException {
        Scn currentScn = archiveLogOnlyMode
                ? connection.getMaxArchiveLogScn(archiveLogDestinationName)
                : connection.getCurrentScn();
        streamingMetrics.setCurrentScn(currentScn);

        Scn topScnToMine = startScn.add(Scn.valueOf(streamingMetrics.getBatchSize()));

        // adjust batch size
        boolean topMiningScnInFarFuture = false;
        if (topScnToMine.subtract(currentScn).compareTo(Scn.valueOf(defaultBatchSize)) > 0) {
            streamingMetrics.changeBatchSize(false, lobEnabled);
            topMiningScnInFarFuture = true;
        }
        if (currentScn.subtract(topScnToMine).compareTo(Scn.valueOf(defaultBatchSize)) > 0) {
            streamingMetrics.changeBatchSize(true, lobEnabled);
        }

        // adjust sleeping time to reduce DB impact
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
            LOGGER.debug("Using Top SCN calculation {} as end SCN.", topScnToMine);
            return topScnToMine;
        }
    }

    /**
     * It is critical to flush LogWriter(s) buffer
     *
     * @param connection container level database connection
     * @param config configuration
     * @param isRac true if this is the RAC system
     * @param racHosts set of RAC host
     * @throws SQLException exception
     */
    static void flushLogWriter(OracleConnection connection, JdbcConfiguration config,
                               boolean isRac, Set<String> racHosts)
            throws SQLException {
        Scn currentScn = connection.getCurrentScn();
        if (isRac) {
            flushRacLogWriters(currentScn, config, racHosts);
        }
        else {
            LOGGER.trace("Updating {} with SCN {}", SqlUtils.LOGMNR_FLUSH_TABLE, currentScn);
            executeCallableStatement(connection, SqlUtils.UPDATE_FLUSH_TABLE + currentScn);
            connection.commit();
        }
    }

    /**
     * Get the database time in the time zone of the system this database is running on
     *
     * @param connection connection
     * @return the database system time
     */
    static OffsetDateTime getSystime(OracleConnection connection) throws SQLException {
        return connection.queryAndMap(SqlUtils.SELECT_SYSTIMESTAMP, rs -> {
            if (rs.next()) {
                return rs.getObject(1, OffsetDateTime.class);
            }
            else {
                return null;
            }
        });
    }

    /**
     * This method builds mining view to query changes from.
     * This view is built for online redo log files.
     * It starts log mining session.
     * It uses data dictionary objects, incorporated in previous steps.
     * It tracks DDL changes and mines committed data only.
     *
     * @param connection container level database connection
     * @param startScn   the SCN to mine from
     * @param endScn     the SCN to mine to
     * @param strategy this is about dictionary location
     * @param isContinuousMining works < 19 version only
     * @param streamingMetrics the streaming metrics
     * @throws SQLException if anything unexpected happens
     */
    static void startLogMining(OracleConnection connection, Scn startScn, Scn endScn,
                               OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining, OracleStreamingChangeEventSourceMetrics streamingMetrics)
            throws SQLException {
        LOGGER.trace("Starting log mining startScn={}, endScn={}, strategy={}, continuous={}", startScn, endScn, strategy, isContinuousMining);
        String statement = SqlUtils.startLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
        try {
            Instant start = Instant.now();
            executeCallableStatement(connection, statement);
            streamingMetrics.addCurrentMiningSessionStart(Duration.between(start, Instant.now()));
        }
        catch (SQLException e) {
            // Capture database state before throwing exception
            logDatabaseState(connection);
            throw e;
        }
        // todo dbms_logmnr.STRING_LITERALS_IN_STMT?
        // todo If the log file is corrupted/bad, logmnr will not be able to access it, we have to switch to another one?
    }

    /**
     * This method query the database to get CURRENT online redo log file(s). Multiple is applicable for RAC systems.
     * @param connection connection to reuse
     * @return full redo log file name(s), including path
     * @throws SQLException if anything unexpected happens
     */
    static Set<String> getCurrentRedoLogFiles(OracleConnection connection) throws SQLException {
        final Set<String> fileNames = new HashSet<>();
        connection.query(SqlUtils.currentRedoNameQuery(), rs -> {
            while (rs.next()) {
                fileNames.add(rs.getString(1));
            }
        });
        LOGGER.trace(" Current Redo log fileNames: {} ", fileNames);
        return fileNames;
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection container level database connection
     * @param archiveLogRetention duration that archive logs are mined
     * @param archiveDestinationName configured archive destination name to use, may be {@code null}
     * @return oldest SCN from online redo log
     * @throws SQLException if anything unexpected happens
     */
    static Scn getFirstOnlineLogScn(OracleConnection connection, Duration archiveLogRetention, String archiveDestinationName) throws SQLException {
        LOGGER.trace("Getting first scn of all online logs");
        try (Statement s = connection.connection(false).createStatement()) {
            try (ResultSet rs = s.executeQuery(SqlUtils.oldestFirstChangeQuery(archiveLogRetention, archiveDestinationName))) {
                rs.next();
                Scn firstScnOfOnlineLog = Scn.valueOf(rs.getString(1));
                LOGGER.trace("First SCN in online logs is {}", firstScnOfOnlineLog);
                return firstScnOfOnlineLog;
            }
        }
    }

    /**
     * Sets NLS parameters for mining session.
     *
     * @param connection session level database connection
     * @throws SQLException if anything unexpected happens
     */
    static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(SqlUtils.NLS_SESSION_PARAMETERS);
        // This is necessary so that TIMESTAMP WITH LOCAL TIME ZONE get returned in UTC
        connection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");
    }

    /**
     * This fetches online redo log statuses
     * @param connection privileged connection
     * @return REDO LOG statuses Map, where key is the REDO name and value is the status
     * @throws SQLException if anything unexpected happens
     */
    private static Map<String, String> getRedoLogStatus(OracleConnection connection) throws SQLException {
        return getMap(connection, SqlUtils.redoLogStatusQuery(), UNKNOWN);
    }

    /**
     * This fetches REDO LOG switch count for the last day
     *
     * @param connection privileged connection
     * @param archiveDestinationName configured archive destination name, may be {@code null}
     * @return counter
     */
    private static int getSwitchCount(OracleConnection connection, String archiveDestinationName) {
        try {
            Map<String, String> total = getMap(connection, SqlUtils.switchHistoryQuery(archiveDestinationName), UNKNOWN);
            if (total != null && total.get(TOTAL) != null) {
                return Integer.parseInt(total.get(TOTAL));
            }
        }
        catch (Exception e) {
            LOGGER.error("Cannot get switch counter", e);
        }
        return 0;
    }

    /**
     * Oracle RAC has one LogWriter per node (instance), we have to flush them all
     * We cannot use a query like from gv_instance view to get all the nodes, because not all nodes could be load balanced.
     * We also cannot rely on connection factory, because it may return connection to the same instance multiple times
     * Instead we are asking node ip list from configuration
     */
    private static void flushRacLogWriters(Scn currentScn, JdbcConfiguration config, Set<String> racHosts) {
        Instant startTime = Instant.now();
        if (racHosts.isEmpty()) {
            throw new RuntimeException("No RAC node ip addresses were supplied in the configuration");
        }

        // todo: Ugly, but, using one factory.connect() in the loop, it may always connect the same node with badly configured load balancer
        boolean errors = false;
        for (String host : racHosts) {
            try {
                OracleConnection conn = racFlushConnections.get(host);
                if (conn == null) {
                    LOGGER.warn("Connection to the node {} was not instantiated", host);
                    errors = true;
                    continue;
                }
                LOGGER.trace("Flushing Log Writer buffer of node {}", host);
                executeCallableStatement(conn, SqlUtils.UPDATE_FLUSH_TABLE + currentScn);
                conn.commit();
            }
            catch (Exception e) {
                LOGGER.warn("Cannot flush Log Writer buffer of the node {} due to {}", host, e);
                errors = true;
            }
        }

        if (errors) {
            instantiateFlushConnections(config, racHosts);
            LOGGER.warn("Not all LogWriter buffers were flushed. Sleeping for 3 seconds to let Oracle do the flush.", racHosts);
            Metronome metronome = Metronome.sleeper(Duration.ofMillis(3000), Clock.system());
            try {
                metronome.pause();
            }
            catch (InterruptedException e) {
                LOGGER.warn("Metronome was interrupted");
            }
        }

        LOGGER.trace("Flushing RAC Log Writers took {} ", Duration.between(startTime, Instant.now()));
    }

    // todo use pool
    private static OracleConnection createFlushConnection(JdbcConfiguration config, String host) throws SQLException {
        JdbcConfiguration hostConfig = JdbcConfiguration.adapt(config.edit().with(JdbcConfiguration.HOSTNAME, host).build());
        LOGGER.debug("Creating RAC flush connection to '{}:{}'", hostConfig.getHostname(), hostConfig.getPort());
        OracleConnection connection = new OracleConnection(hostConfig, () -> LogMinerHelper.class.getClassLoader());
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * This method validates the supplemental logging configuration for the source database.
     *
     * @param connection oracle connection on LogMiner level
     * @param pdbName pdb name
     * @param schema oracle schema
     * @throws SQLException if anything unexpected happens
     */
    static void checkSupplementalLogging(OracleConnection connection, String pdbName, OracleDatabaseSchema schema) throws SQLException {
        try {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }

            // Check if ALL supplemental logging is enabled at the database
            Map<String, String> globalAll = getMap(connection, SqlUtils.databaseSupplementalLoggingAllCheckQuery(), UNKNOWN);
            if ("NO".equalsIgnoreCase(globalAll.get("KEY"))) {
                // Check if MIN supplemental logging is enabled at the database
                Map<String, String> globalMin = getMap(connection, SqlUtils.databaseSupplementalLoggingMinCheckQuery(), UNKNOWN);
                if ("NO".equalsIgnoreCase(globalMin.get("KEY"))) {
                    throw new DebeziumException("Supplemental logging not properly configured.  Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
                }

                // If ALL supplemental logging is not enabled, then each monitored table should be set to ALL COLUMNS
                for (TableId tableId : schema.getTables().tableIds()) {
                    if (!isTableSupplementalLogDataAll(connection, tableId)) {
                        throw new DebeziumException("Supplemental logging not configured for table " + tableId + ".  " +
                                "Use command: ALTER TABLE " + tableId.schema() + "." + tableId.table() + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
                    }
                }
            }
        }
        finally {
            if (pdbName != null) {
                connection.resetSessionToCdb();
            }
        }
    }

    static boolean isTableSupplementalLogDataAll(OracleConnection connection, TableId tableId) throws SQLException {
        return connection.queryAndMap(SqlUtils.tableSupplementalLoggingCheckQuery(tableId), (rs) -> {
            while (rs.next()) {
                if (ALL_COLUMN_LOGGING.equals(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * This call completes LogMiner session.
     * Complete gracefully.
     *
     * @param connection container level database connection
     */
    public static void endMining(OracleConnection connection) {
        String stopMining = SqlUtils.END_LOGMNR;
        try {
            executeCallableStatement(connection, stopMining);
        }
        catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("LogMiner session was already closed");
            }
            else {
                LOGGER.error("Cannot close LogMiner session gracefully: {}", e);
            }
        }
    }

    /**
     * This method substitutes CONTINUOUS_MINE functionality
     *
     * @param connection connection
     * @param lastProcessedScn current offset
     * @param archiveLogRetention the duration that archive logs will be mined
     * @param archiveLogOnlyMode true to mine only archive lgos, false to mine all available logs
     * @param archiveDestinationName configured archive log destination name to use, may be {@code null}
     * @throws SQLException if anything unexpected happens
     */
    // todo: check RAC resiliency
    public static void setLogFilesForMining(OracleConnection connection, Scn lastProcessedScn, Duration archiveLogRetention, boolean archiveLogOnlyMode,
                                            String archiveDestinationName)
            throws SQLException {
        removeLogFilesFromMining(connection);

        List<LogFile> logFilesForMining = getLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName);
        if (!logFilesForMining.stream().anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0)) {
            Scn minScn = logFilesForMining.stream()
                    .map(LogFile::getFirstScn)
                    .min(Scn::compareTo)
                    .orElse(Scn.NULL);

            if ((minScn.isNull() || logFilesForMining.isEmpty()) && archiveLogOnlyMode) {
                throw new DebeziumException("The log.mining.archive.log.only mode was recently enabled and the offset SCN " +
                        lastProcessedScn + "is not yet in any available archive logs. " +
                        "Please perform an Oracle log switch and restart the connector.");
            }
            throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        }

        List<String> logFilesNames = logFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
        for (String file : logFilesNames) {
            LOGGER.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
        }

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    /**
     * This method calculates SCN as a watermark to abandon long lasting transactions.
     * The criteria is don't let offset scn go out of archives older given number of hours
     *
     * @param connection connection
     * @param offsetScn current offset scn
     * @param transactionRetention duration to tolerate long running transactions
     * @return optional SCN as a watermark for abandonment
     */
    public static Optional<Scn> getLastScnToAbandon(OracleConnection connection, Scn offsetScn, Duration transactionRetention) {
        try {
            String query = SqlUtils.diffInDaysQuery(offsetScn);
            Float diffInDays = (Float) getSingleResult(connection, query, DATATYPE.FLOAT);
            if (diffInDays != null && (diffInDays * 24) > transactionRetention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference due to {}", e);
            return Optional.of(offsetScn);
        }
    }

    static void logWarn(OracleStreamingChangeEventSourceMetrics streamingMetrics, String format, Object... args) {
        LOGGER.warn(format, args);
        streamingMetrics.incrementWarningCount();
    }

    static void logError(OracleStreamingChangeEventSourceMetrics streamingMetrics, String format, Object... args) {
        LOGGER.error(format, args);
        streamingMetrics.incrementErrorCount();
    }

    /**
     * Get all log files that should be mined.
     *
     * @param connection database connection
     * @param offsetScn offset system change number
     * @param archiveLogRetention duration that archive logs should be mined
     * @param archiveLogOnlyMode true to mine only archive logs, false to mine all available logs
     * @param archiveDestinationName archive destination to use, may be {@code null}
     * @return list of log files
     * @throws SQLException if a database exception occurs
     */
    public static List<LogFile> getLogFilesForOffsetScn(OracleConnection connection, Scn offsetScn, Duration archiveLogRetention, boolean archiveLogOnlyMode,
                                                        String archiveDestinationName)
            throws SQLException {
        LOGGER.trace("Getting logs to be mined for offset scn {}", offsetScn);

        final List<LogFile> logFiles = new ArrayList<>();
        final List<LogFile> onlineLogFiles = new ArrayList<>();
        final List<LogFile> archivedLogFiles = new ArrayList<>();

        connection.query(SqlUtils.allMinableLogsQuery(offsetScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName), rs -> {
            while (rs.next()) {
                String fileName = rs.getString(1);
                Scn firstScn = getScnFromString(rs.getString(2));
                Scn nextScn = getScnFromString(rs.getString(3));
                String status = rs.getString(5);
                String type = rs.getString(6);
                Long sequence = rs.getLong(7);
                if ("ARCHIVED".equals(type)) {
                    // archive log record
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE);
                    if (logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.trace("Archive log {} with SCN range {} to {} sequence {} to be added.", fileName, firstScn, nextScn, sequence);
                        archivedLogFiles.add(logFile);
                    }
                }
                else if ("ONLINE".equals(type)) {
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, CURRENT.equalsIgnoreCase(status));
                    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.", fileName, firstScn, nextScn, status, sequence);
                        onlineLogFiles.add(logFile);
                    }
                    else {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.", fileName, firstScn, nextScn, status, sequence);
                    }
                }
            }
        });

        // DBZ-3563
        // To avoid duplicate log files (ORA-01289 cannot add duplicate logfile)
        // Remove the archive log which has the same sequence number.
        for (LogFile redoLog : onlineLogFiles) {
            archivedLogFiles.removeIf(f -> {
                if (f.getSequence().equals(redoLog.getSequence())) {
                    LOGGER.trace("Removing archive log {} with duplicate sequence {} to {}", f.getFileName(), f.getSequence(), redoLog.getFileName());
                    return true;
                }
                return false;
            });
        }
        logFiles.addAll(archivedLogFiles);
        logFiles.addAll(onlineLogFiles);

        return logFiles;
    }

    private static Scn getScnFromString(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return Scn.MAX;
        }
        return Scn.valueOf(value);
    }

    /**
     * Helper method that will dump the state of various critical tables used by the LogMiner implementation
     * to derive state about which logs are to be mined and processed by the Oracle LogMiner session.
     *
     * @param connection the database connection
     */
    private static void logDatabaseState(OracleConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Configured redo logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGFILE");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain redo log table entries", e);
            }
            LOGGER.debug("Available archive logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$ARCHIVED_LOG");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain archive log table entries", e);
            }
            LOGGER.debug("Available logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOG");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain log table entries", e);
            }
            LOGGER.debug("Log history last 24 hours:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOG_HISTORY WHERE FIRST_TIME >= SYSDATE - 1");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain log history", e);
            }
            LOGGER.debug("Log entries registered with LogMiner are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_LOGS");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain registered logs with LogMiner", e);
            }
            LOGGER.debug("Log mining session parameters are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_PARAMETERS");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain log mining session parameters", e);
            }
        }
    }

    /**
     * Helper method that dumps the result set of an arbitrary SQL query to the connector's logs.
     *
     * @param connection the database connection
     * @param query the query to execute
     * @throws SQLException thrown if an exception occurs performing a SQL operation
     */
    private static void logQueryResults(OracleConnection connection, String query) throws SQLException {
        connection.query(query, rs -> {
            int columns = rs.getMetaData().getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int index = 1; index <= columns; ++index) {
                columnNames.add(rs.getMetaData().getColumnName(index));
            }
            LOGGER.debug("{}", columnNames);
            while (rs.next()) {
                List<Object> columnValues = new ArrayList<>();
                for (int index = 1; index <= columns; ++index) {
                    columnValues.add(rs.getObject(index));
                }
                LOGGER.debug("{}", columnValues);
            }
        });
    }

    /**
     * This method removes all added log files from mining
     * @param conn connection
     * @throws SQLException something happened
     */
    public static void removeLogFilesFromMining(OracleConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.connection(false).prepareStatement(SqlUtils.FILES_FOR_MINING);
                ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                executeCallableStatement(conn, SqlUtils.deleteLogFileStatement(fileName));
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        }
    }

    private static void executeCallableStatement(OracleConnection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.connection(false).prepareCall(statement)) {
            s.execute();
        }
    }

    public static Map<String, String> getMap(OracleConnection connection, String query, String nullReplacement) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        try (
                PreparedStatement statement = connection.connection(false).prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String value = rs.getString(2);
                value = value == null ? nullReplacement : value;
                result.put(rs.getString(1), value);
            }
            return result;
        }
    }

    public static Object getSingleResult(OracleConnection connection, String query, DATATYPE type) throws SQLException {
        try (PreparedStatement statement = connection.connection(false).prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                switch (type) {
                    case LONG:
                        return rs.getLong(1);
                    case TIMESTAMP:
                        return rs.getTimestamp(1);
                    case STRING:
                        return rs.getString(1);
                    case FLOAT:
                        return rs.getFloat(1);
                }
            }
            return null;
        }
    }

    /**
     * Returns a 0-based index offset for the column name in the relational table.
     *
     * @param columnName the column name, should not be {@code null}.
     * @param table the relational table, should not be {@code null}.
     * @return the 0-based index offset for the column name
     */
    public static int getColumnIndexByName(String columnName, Table table) {
        final Column column = table.columnWithName(columnName);
        if (column == null) {
            throw new DebeziumException("No column '" + columnName + "' found in table '" + table.id() + "'");
        }
        // want to return a 0-based index and column positions are 1-based
        return column.position() - 1;
    }
}
