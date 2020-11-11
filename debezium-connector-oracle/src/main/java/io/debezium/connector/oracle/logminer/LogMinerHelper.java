/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
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

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * This class contains methods to configure and manage Log Miner utility
 */
public class LogMinerHelper {

    private final static String UNKNOWN = "unknown";
    private static final String TOTAL = "TOTAL";
    private final static Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

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
                    LOGGER.warn("Cannot close exist RAC flush connection", e);
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
     * @param connection connection to the database as log miner user (connection to the container)
     * @throws SQLException any exception
     */
    static void buildDataDictionary(Connection connection) throws SQLException {
        executeCallableStatement(connection, SqlUtils.BUILD_DICTIONARY);
    }

    /**
     * This method returns current SCN from the database
     *
     * @param connection container level database connection
     * @return current SCN
     * @throws SQLException if anything unexpected happens
     */
    public static long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(SqlUtils.currentScnQuery())) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            return rs.getLong(1);
        }
    }

    static void createFlushTable(Connection connection) throws SQLException {
        String tableExists = (String) getSingleResult(connection, SqlUtils.tableExistsQuery(SqlUtils.LOGMNR_FLUSH_TABLE), DATATYPE.STRING);
        if (tableExists == null) {
            executeCallableStatement(connection, SqlUtils.CREATE_FLUSH_TABLE);
        }

        String recordExists = (String) getSingleResult(connection, SqlUtils.FLUSH_TABLE_NOT_EMPTY, DATATYPE.STRING);
        if (recordExists == null) {
            executeCallableStatement(connection, SqlUtils.INSERT_FLUSH_TABLE);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    static void createLogMiningHistoryObjects(Connection connection, String historyTableName) throws SQLException {

        String tableExists = (String) getSingleResult(connection, SqlUtils.tableExistsQuery(SqlUtils.LOGMNR_HISTORY_TEMP_TABLE), DATATYPE.STRING);
        if (tableExists == null) {
            executeCallableStatement(connection, SqlUtils.logMiningHistoryDdl(SqlUtils.LOGMNR_HISTORY_TEMP_TABLE));
        }
        tableExists = (String) getSingleResult(connection, SqlUtils.tableExistsQuery(historyTableName), DATATYPE.STRING);
        if (tableExists == null) {
            executeCallableStatement(connection, SqlUtils.logMiningHistoryDdl(historyTableName));
        }
        String sequenceExists = (String) getSingleResult(connection, SqlUtils.LOGMINING_HISTORY_SEQUENCE_EXISTS, DATATYPE.STRING);
        if (sequenceExists == null) {
            executeCallableStatement(connection, SqlUtils.CREATE_LOGMINING_HISTORY_SEQUENCE);
        }
    }

    static void deleteOutdatedHistory(Connection connection, long retention) throws SQLException {
        Set<String> tableNames = getMap(connection, SqlUtils.getHistoryTableNamesQuery(), "-1").keySet();
        for (String tableName : tableNames) {
            long hoursAgo = SqlUtils.parseRetentionFromName(tableName);
            if (hoursAgo > retention) {
                LOGGER.info("Deleting history table {}", tableName);
                executeCallableStatement(connection, SqlUtils.dropHistoryTableStatement(tableName));
            }
        }
    }

    /**
     * This method returns next SCN for mining  and also updates MBean metrics
     * We use a configurable limit, because the larger mining range, the slower query from Log Miner content view.
     * In addition capturing unlimited number of changes can blow up Java heap.
     * Gradual querying helps to catch up faster after long delays in mining.
     *
     * @param connection container level database connection
     * @param startScn start SCN
     * @param metrics MBean accessible metrics
     * @return next SCN to mine up to
     * @throws SQLException if anything unexpected happens
     */
    static long getEndScn(Connection connection, long startScn, LogMinerMetrics metrics) throws SQLException {
        long currentScn = getCurrentScn(connection);
        metrics.setCurrentScn(currentScn);

        long topScnToMine = startScn + metrics.getBatchSize();

        // adjust batch size
        boolean topMiningScnInFarFuture = false;
        if ((topScnToMine - currentScn) > LogMinerMetrics.DEFAULT_BATCH_SIZE) {
            metrics.changeBatchSize(false);
            topMiningScnInFarFuture = true;
        }
        if ((currentScn - topScnToMine) > LogMinerMetrics.DEFAULT_BATCH_SIZE) {
            metrics.changeBatchSize(true);
        }

        // adjust sleeping time to reduce DB impact
        if (currentScn < topScnToMine) {
            if (!topMiningScnInFarFuture) {
                metrics.changeSleepingTime(true);
            }
            return currentScn;
        }
        else {
            metrics.changeSleepingTime(false);
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
    static void flushLogWriter(Connection connection, JdbcConfiguration config,
                               boolean isRac, Set<String> racHosts)
            throws SQLException {
        long currentScn = getCurrentScn(connection);
        if (isRac) {
            flushRacLogWriters(currentScn, config, racHosts);
        }
        else {
            executeCallableStatement(connection, SqlUtils.UPDATE_FLUSH_TABLE + currentScn);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    /**
     * Calculate time difference between database and connector timers. It could be negative if DB time is ahead.
     * @param connection connection
     * @return the time difference as a {@link Duration}
     */
    static Duration getTimeDifference(Connection connection) throws SQLException {
        Timestamp dbCurrentMillis = (Timestamp) getSingleResult(connection, SqlUtils.CURRENT_TIMESTAMP, DATATYPE.TIMESTAMP);
        if (dbCurrentMillis == null) {
            return Duration.ZERO;
        }
        Instant fromDb = dbCurrentMillis.toInstant();
        Instant now = Instant.now();
        return Duration.between(fromDb, now);
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
     * @throws SQLException if anything unexpected happens
     */
    static void startLogMining(Connection connection, Long startScn, Long endScn,
                               OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining)
            throws SQLException {
        String statement = SqlUtils.startLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
        executeCallableStatement(connection, statement);
        // todo dbms_logmnr.STRING_LITERALS_IN_STMT?
        // todo If the log file is corrupted/bad, logmnr will not be able to access it, we have to switch to another one?
    }

    /**
     * This method query the database to get CURRENT online redo log file(s). Multiple is applicable for RAC systems.
     * @param connection connection to reuse
     * @param metrics MBean accessible metrics
     * @return full redo log file name(s), including path
     * @throws SQLException if anything unexpected happens
     */
    static Set<String> getCurrentRedoLogFiles(Connection connection, LogMinerMetrics metrics) throws SQLException {
        Set<String> fileNames = new HashSet<>();
        try (PreparedStatement st = connection.prepareStatement(SqlUtils.currentRedoNameQuery()); ResultSet result = st.executeQuery()) {
            while (result.next()) {
                fileNames.add(result.getString(1));
                LOGGER.trace(" Current Redo log fileName: {} ", fileNames);
            }
        }

        updateRedoLogMetrics(connection, metrics, fileNames);
        return fileNames;
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection container level database connection
     * @return oldest SCN from online redo log
     * @throws SQLException if anything unexpected happens
     */
    static long getFirstOnlineLogScn(Connection connection) throws SQLException {
        LOGGER.trace("getting first scn of all online logs");
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery(SqlUtils.oldestFirstChangeQuery());
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        res.close();
        return firstScnOfOnlineLog;
    }

    /**
     * Sets NLS parameters for mining session.
     *
     * @param connection session level database connection
     * @throws SQLException if anything unexpected happens
     */
    static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(SqlUtils.NLS_SESSION_PARAMETERS);
    }

    /**
     * This is to update MBean metrics associated with REDO LOG groups
     * @param connection connection
     * @param fileNames name of current REDO LOG files
     * @param metrics current metrics
     */
    private static void updateRedoLogMetrics(Connection connection, LogMinerMetrics metrics, Set<String> fileNames) {
        try {
            // update metrics
            Map<String, String> logStatuses = getRedoLogStatus(connection);
            metrics.setRedoLogStatus(logStatuses);

            int counter = getSwitchCount(connection);
            metrics.setSwitchCount(counter);
            metrics.setCurrentLogFileName(fileNames);
        }
        catch (SQLException e) {
            LOGGER.error("Cannot update metrics");
        }
    }

    /**
     * This fetches online redo log statuses
     * @param connection privileged connection
     * @return REDO LOG statuses Map, where key is the REDO name and value is the status
     * @throws SQLException if anything unexpected happens
     */
    private static Map<String, String> getRedoLogStatus(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.redoLogStatusQuery(), UNKNOWN);
    }

    /**
     * This fetches REDO LOG switch count for the last day
     * @param connection privileged connection
     * @return counter
     */
    private static int getSwitchCount(Connection connection) {
        try {
            Map<String, String> total = getMap(connection, SqlUtils.switchHistoryQuery(), UNKNOWN);
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
    private static void flushRacLogWriters(long currentScn, JdbcConfiguration config, Set<String> racHosts) {
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
                executeCallableStatement(conn.connection(), SqlUtils.UPDATE_FLUSH_TABLE + currentScn);
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
        JdbcConfiguration hostConfig = JdbcConfiguration.adapt(config.edit().with(JdbcConfiguration.DATABASE, host).build());
        OracleConnection connection = new OracleConnection(hostConfig, () -> LogMinerHelper.class.getClassLoader());
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * This method checks if supplemental logging was set on the database level. This is critical check, cannot work if not.
     * @param connection oracle connection on logminer level
     * @param pdbName pdb name
     * @throws SQLException if anything unexpected happens
     */
    static void checkSupplementalLogging(OracleConnection connection, String pdbName) throws SQLException {
        try {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }

            Map<String, String> globalLogging = getMap(connection.connection(false), SqlUtils.supplementalLoggingCheckQuery(), UNKNOWN);
            if ("no".equalsIgnoreCase(globalLogging.get("KEY"))) {
                throw new RuntimeException("Supplemental logging was not set. Use command: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            }
        }
        finally {
            if (pdbName != null) {
                connection.resetSessionToCdb();
            }
        }
    }

    /**
     * This call completes log miner session.
     * Complete gracefully.
     *
     * @param connection container level database connection
     */
    public static void endMining(Connection connection) {
        String stopMining = SqlUtils.END_LOGMNR;
        try {
            executeCallableStatement(connection, stopMining);
        }
        catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("Log Miner session was already closed");
            }
            else {
                LOGGER.error("Cannot close Log Miner session gracefully: {}", e);
            }
        }
    }

    /**
     * This method substitutes CONTINUOUS_MINE functionality
     * @param connection connection
     * @param lastProcessedScn current offset
     * @throws SQLException if anything unexpected happens
     */
    // todo: check RAC resiliency
    public static void setRedoLogFilesForMining(Connection connection, Long lastProcessedScn) throws SQLException {

        removeLogFilesFromMining(connection);

        Map<String, Long> onlineLogFilesForMining = getOnlineLogFilesForOffsetScn(connection, lastProcessedScn);
        Map<String, Long> archivedLogFilesForMining = getArchivedLogFilesForOffsetScn(connection, lastProcessedScn);

        if (onlineLogFilesForMining.size() + archivedLogFilesForMining.size() == 0) {
            throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        }

        List<String> logFilesNames = onlineLogFilesForMining.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        // remove duplications
        List<String> archivedLogFiles = archivedLogFilesForMining.entrySet().stream()
                .filter(e -> !onlineLogFilesForMining.values().contains(e.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        logFilesNames.addAll(archivedLogFiles);

        for (String file : logFilesNames) {
            String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
            LOGGER.trace("add log file to the mining session = {}", file);
        }

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    /**
     * This method calculates SCN as a watermark to abandon long lasting transactions.
     * The criteria is don't let offset scn go out of archives older given number of hours
     *
     * @param connection connection
     * @param offsetScn current offset scn
     * @param hoursToKeepTransaction hours to tolerate long transaction
     * @return optional SCN as a watermark for abandonment
     */
    public static Optional<Long> getLastScnToAbandon(Connection connection, Long offsetScn, int hoursToKeepTransaction) {
        try {
            String query = SqlUtils.diffInDaysQuery(offsetScn);
            Float diffInDays = (Float) getSingleResult(connection, query, DATATYPE.FLOAT);
            if (diffInDays != null && (diffInDays * 24) > hoursToKeepTransaction) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference due to {}", e);
            return Optional.of(offsetScn);
        }
    }

    static void logWarn(TransactionalBufferMetrics metrics, String format, Object... args) {
        LOGGER.warn(format, args);
        metrics.incrementWarningCounter();
    }

    static void logError(TransactionalBufferMetrics metrics, String format, Object... args) {
        LOGGER.error(format, args);
        metrics.incrementErrorCounter();
    }

    /**
     * get size of online REDO groups
     * @param connection connection
     * @return size
     */
    private static int getRedoLogGroupSize(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.allOnlineLogsQuery(), "-1").size();
    }

    /**
     * This method returns all online log files, starting from one which contains offset SCN and ending with one containing largest SCN
     * 18446744073709551615 on Ora 19c is the max value of the nextScn in the current redo todo replace all Long with BigInteger for SCN
     */
    public static Map<String, Long> getOnlineLogFilesForOffsetScn(Connection connection, Long offsetScn) throws SQLException {
        Map<String, String> redoLogFiles = getMap(connection, SqlUtils.allOnlineLogsQuery(), "-1");
        return redoLogFiles.entrySet().stream()
                .filter(entry -> new BigInteger(entry.getValue()).longValue() > offsetScn || new BigInteger(entry.getValue()).longValue() == -1).collect(Collectors
                        .toMap(Map.Entry::getKey, e -> new BigInteger(e.getValue()).longValue() == -1 ? Long.MAX_VALUE : new BigInteger(e.getValue()).longValue()));
    }

    /**
     * This method returns all archived log files for one day, containing given offset scn
     * @param connection    connection
     * @param offsetScn     offset scn
     * @return              Map of archived files
     * @throws SQLException if something happens
     */
    public static Map<String, Long> getArchivedLogFilesForOffsetScn(Connection connection, Long offsetScn) throws SQLException {
        Map<String, String> redoLogFiles = getMap(connection, SqlUtils.oneDayArchivedLogsQuery(offsetScn), "-1");
        return redoLogFiles.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, e -> new BigDecimal(e.getValue()).longValue() == -1 ? Long.MAX_VALUE : new BigDecimal(e.getValue()).longValue()));
    }

    /**
     * This method removes all added log files from mining
     * @param conn connection
     * @throws SQLException something happened
     */
    public static void removeLogFilesFromMining(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SqlUtils.FILES_FOR_MINING);
                ResultSet result = ps.executeQuery()) {
            while (result.next()) {
                String fileName = result.getString(1);
                executeCallableStatement(conn, SqlUtils.deleteLogFileStatement(fileName));
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        }
    }

    private static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.prepareCall(statement)) {
            s.execute();
        }
    }

    public static Map<String, String> getMap(Connection connection, String query, String nullReplacement) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        try (
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String value = rs.getString(2);
                value = value == null ? nullReplacement : value;
                result.put(rs.getString(1), value);
            }
            return result;
        }
    }

    public static Object getSingleResult(Connection connection, String query, DATATYPE type) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(query);
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
}
