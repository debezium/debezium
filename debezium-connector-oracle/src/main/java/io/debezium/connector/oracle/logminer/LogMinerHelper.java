/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

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
import io.debezium.jdbc.JdbcConnection;

/**
 * This class contains methods to configure and manage Log Miner utility
 */
public class LogMinerHelper {

    private final static String UNKNOWN = "unknown";
    private final static Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    private enum DATATYPE {
        LONG,
        TIMESTAMP,
        STRING
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
                ResultSet rs = statement.executeQuery(SqlUtils.CURRENT_SCN)) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            long currentScn = rs.getLong(1);
            rs.close();
            return currentScn;
        }
    }

    static void createAuditTable(Connection connection) throws SQLException {
        String tableExists = (String) getSingleResult(connection, SqlUtils.AUDIT_TABLE_EXISTS, DATATYPE.STRING);
        if (tableExists == null) {
            executeCallableStatement(connection, SqlUtils.CREATE_AUDIT_TABLE);
        }

        String recordExists = (String) getSingleResult(connection, SqlUtils.AUDIT_TABLE_RECORD_EXISTS, DATATYPE.STRING);
        if (recordExists == null) {
            executeCallableStatement(connection, SqlUtils.INSERT_AUDIT_TABLE);
            if (!connection.getAutoCommit()) {
                connection.commit();
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
     * @param metrics MBean accessible metrics
     * @param startScn start SCN
     * @return next SCN to mine to
     * @throws SQLException if anything unexpected happens
     */
    static long getEndScn(Connection connection, long startScn, LogMinerMetrics metrics) throws SQLException {
        long currentScn = getCurrentScn(connection);
        metrics.setCurrentScn(currentScn);
        int miningDiapason = metrics.getBatchSize();

        // it is critical to flush LogWriter buffer
        executeCallableStatement(connection, SqlUtils.UPDATE_AUDIT_TABLE + currentScn);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }

        // adjust sleeping time to optimize DB impact and catchup faster when behind
        boolean isNextScnCloseToDbCurrent = currentScn < (startScn + miningDiapason);
        metrics.changeSleepingTime(isNextScnCloseToDbCurrent);

        return isNextScnCloseToDbCurrent ? currentScn : startScn + miningDiapason;
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
    static void startOnlineMining(Connection connection, Long startScn, Long endScn,
                                  OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining)
            throws SQLException {
        String statement = SqlUtils.getStartLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
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
        String checkQuery = SqlUtils.CURRENT_REDO_LOG_NAME;

        Set<String> fileNames = new HashSet<>();
        try (PreparedStatement st = connection.prepareStatement(checkQuery); ResultSet result = st.executeQuery()) {
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
        ResultSet res = s.executeQuery(SqlUtils.OLDEST_FIRST_CHANGE);
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
        return getMap(connection, SqlUtils.REDO_LOGS_STATUS, UNKNOWN);
    }

    /**
     * This fetches REDO LOG switch count for the last day
     * @param connection privileged connection
     * @return counter
     */
    private static int getSwitchCount(Connection connection) {
        try {
            Map<String, String> total = getMap(connection, SqlUtils.SWITCH_HISTORY_TOTAL_COUNT, UNKNOWN);
            if (total != null && total.get("total") != null) {
                return Integer.parseInt(total.get("total"));
            }
        }
        catch (Exception e) {
            LOGGER.error("Cannot get switch counter due to the {}", e);
        }
        return 0;
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

            final String key = "KEY";
            String validateGlobalLogging = "SELECT '" + key + "', " + " SUPPLEMENTAL_LOG_DATA_ALL from V$DATABASE";
            Map<String, String> globalLogging = getMap(connection.connection(false), validateGlobalLogging, UNKNOWN);
            if ("no".equalsIgnoreCase(globalLogging.get(key))) {
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
    static void endMining(Connection connection) {
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
     * This method substitutes CONTINUOUS_MINE functionality for online files only
     * @param connection connection
     * @param lastProcessedScn current offset
     * @throws SQLException if anything unexpected happens
     */
    static void setRedoLogFilesForMining(Connection connection, Long lastProcessedScn) throws SQLException {

        Map<String, Long> logFilesForMining = getLogFilesForOffsetScn(connection, lastProcessedScn);
        if (logFilesForMining.isEmpty()) {
            throw new IllegalStateException("The online log files do not contain offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        }

        // The following check seems to be problematic and removal of it does not appear to cause
        // any detrimental impact to the connector's ability to stream changes. This was left as
        // is but commented in case we find a reason for it to exist.
        // int redoLogGroupSize = getRedoLogGroupSize(connection);
        // if (logFilesForMining.size() == redoLogGroupSize) {
        // throw new IllegalStateException("All online log files needed for mining the offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        // }

        List<String> logFilesNamesForMining = logFilesForMining.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        for (String file : logFilesNamesForMining) {
            String addLogFileStatement = SqlUtils.getAddLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
            LOGGER.trace("add log file to the mining session = {}", file);
        }

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesForMining);
    }

    /**
     * This method returns SCN as a watermark to abandon long lasting transactions.
     *
     * @param connection connection
     * @param offsetScn current offset
     * @return Optional last SCN in a redo log
     * @throws SQLException if anything unexpected happens
     */
    static Optional<Long> getLastScnFromTheOldestOnlineRedo(Connection connection, Long offsetScn) throws SQLException {

        Map<String, String> allOnlineRedoLogFiles = getMap(connection, SqlUtils.ALL_ONLINE_LOGS, "-1");
        Map<String, Long> logFilesToMine = getLogFilesForOffsetScn(connection, offsetScn);

        LOGGER.debug("Redo log size = {}, needed for mining files size = {}", allOnlineRedoLogFiles.size(), logFilesToMine.size());
        if (allOnlineRedoLogFiles.size() - logFilesToMine.size() <= 1) {
            List<Long> lastScnsInRedoLogToMine = logFilesToMine.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
            return lastScnsInRedoLogToMine.stream().min(Long::compareTo);
        }
        return Optional.empty();
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
        return getMap(connection, SqlUtils.ALL_ONLINE_LOGS, "-1").size();
    }

    /**
     * This method returns all online log files, starting from one which contains offset SCN and ending with one containing largest SCN
     * 18446744073709551615 on Ora 19c is the max value of the nextScn in the current redo todo replace all Long with BigInteger for SCN
     */
    private static Map<String, Long> getLogFilesForOffsetScn(Connection connection, Long offsetScn) throws SQLException {
        Map<String, String> redoLogFiles = getMap(connection, SqlUtils.ALL_ONLINE_LOGS, "-1");
        return redoLogFiles.entrySet().stream()
                .filter(entry -> new BigInteger(entry.getValue()).longValue() > offsetScn || new BigInteger(entry.getValue()).longValue() == -1).collect(Collectors
                        .toMap(Map.Entry::getKey, e -> new BigInteger(e.getValue()).longValue() == -1 ? Long.MAX_VALUE : new BigInteger(e.getValue()).longValue()));
    }

    private static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.prepareCall(statement)) {
            s.execute();
        }
    }

    private static Map<String, String> getMap(Connection connection, String query, String nullReplacement) throws SQLException {
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

    private static Object getSingleResult(Connection connection, String query, DATATYPE type) throws SQLException {
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
                }
            }
            return null;
        }
    }
}
