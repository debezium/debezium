/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Strings;

/**
 * This class contains methods to configure and manage LogMiner utility
 */
public class LogMinerHelper {

    private static final String CURRENT = "CURRENT";
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    /**
     * This method substitutes CONTINUOUS_MINE functionality
     *
     * @param connection connection
     * @param lastProcessedScn current offset
     * @param archiveLogRetention the duration that archive logs will be mined
     * @param archiveLogOnlyMode true to mine only archive lgos, false to mine all available logs
     * @param archiveDestinationName configured archive log destination name to use, may be {@code null}
     * @param maxRetries the number of retry attempts before giving up and throwing an exception about log state
     * @param initialDelay the initial delay
     * @param maxDelay the maximum delay
     * @throws SQLException if anything unexpected happens
     * @return log files that were added to the current mining session
     */
    // todo: check RAC resiliency
    public static List<LogFile> setLogFilesForMining(OracleConnection connection, Scn lastProcessedScn, Duration archiveLogRetention,
                                                     boolean archiveLogOnlyMode, String archiveDestinationName, int maxRetries,
                                                     Duration initialDelay, Duration maxDelay)
            throws SQLException {
        removeLogFilesFromMining(connection);

        // Restrict max attempts to 0 or greater values (sanity-check)
        // the code will do at least 1 attempt and up to maxAttempts extra polls based on configuration
        final int maxAttempts = Math.max(maxRetries, 0);
        final DelayStrategy retryStrategy = DelayStrategy.exponential(initialDelay, maxDelay);

        // We perform a retry algorithm here as there is a race condition where Oracle may update the V$LOG table
        // but the V$ARCHIVED_LOG lags behind and a single-shot SQL query may return an inconsistent set of results
        // due to Oracle performing the operation non-atomically.
        List<LogFile> logFilesForMining = new ArrayList<>();
        for (int attempt = 0; attempt <= maxAttempts; ++attempt) {
            logFilesForMining.addAll(getLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention,
                    archiveLogOnlyMode, archiveDestinationName));
            // we don't need lastProcessedSCN in the logs, as that one was already processed, but we do want
            // the next SCN to be present, as that is where we'll start processing from.
            if (!hasLogFilesStartingBeforeOrAtScn(logFilesForMining, lastProcessedScn.add(Scn.ONE))) {
                LOGGER.info("No logs available yet (attempt {})...", attempt + 1);
                logFilesForMining.clear();
                retryStrategy.sleepWhen(true);
                continue;
            }

            List<String> logFilesNames = logFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
            for (String file : logFilesNames) {
                LOGGER.trace("Adding log file {} to mining session", file);
                String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
                executeCallableStatement(connection, addLogFileStatement);
            }

            LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}", lastProcessedScn, logFilesNames);
            return logFilesForMining;
        }

        final Scn minScn = getMinimumScn(logFilesForMining);
        if ((minScn.isNull() || logFilesForMining.isEmpty()) && archiveLogOnlyMode) {
            throw new DebeziumException("The log.mining.archive.log.only mode was recently enabled and the offset SCN " +
                    lastProcessedScn + "is not yet in any available archive logs. " +
                    "Please perform an Oracle log switch and restart the connector.");
        }
        throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
    }

    private static boolean hasLogFilesStartingBeforeOrAtScn(List<LogFile> logs, Scn scn) {
        final Map<Integer, List<LogFile>> threadLogs = logs.stream().collect(Collectors.groupingBy(LogFile::getThread));
        for (Map.Entry<Integer, List<LogFile>> entry : threadLogs.entrySet()) {
            if (!entry.getValue().stream().anyMatch(l -> l.getFirstScn().compareTo(scn) <= 0)) {
                LOGGER.debug("Redo thread {} does not yet have any logs before or at SCN {}.", entry.getKey(), scn);
                return false;
            }
        }
        LOGGER.debug("Redo threads {} have logs before or at SCN {}.", threadLogs.keySet(), scn);
        return true;
    }

    private static Scn getMinimumScn(List<LogFile> logs) {
        return logs.stream().map(LogFile::getFirstScn).min(Scn::compareTo).orElse(Scn.NULL);
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
    public static List<LogFile> getLogFilesForOffsetScn(OracleConnection connection, Scn offsetScn, Duration archiveLogRetention,
                                                        boolean archiveLogOnlyMode, String archiveDestinationName)
            throws SQLException {
        LOGGER.trace("Getting logs to be mined for offset scn {}", offsetScn);

        final List<LogFile> logFiles = new ArrayList<>();
        final Set<LogFile> onlineLogFiles = new LinkedHashSet<>();
        final Set<LogFile> archivedLogFiles = new LinkedHashSet<>();

        connection.query(SqlUtils.allMinableLogsQuery(offsetScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName), rs -> {
            while (rs.next()) {
                String fileName = rs.getString(1);
                Scn firstScn = getScnFromString(rs.getString(2));
                Scn nextScn = getScnFromString(rs.getString(3));
                String status = rs.getString(5);
                String type = rs.getString(6);
                BigInteger sequence = new BigInteger(rs.getString(7));
                int thread = rs.getInt(10);
                if ("ARCHIVED".equals(type)) {
                    // archive log record
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE, thread);
                    if (logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.debug("Archive log {} with SCN range {} to {} sequence {} to be added.", fileName, firstScn, nextScn, sequence);
                        archivedLogFiles.add(logFile);
                    }
                }
                else if ("ONLINE".equals(type)) {
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, CURRENT.equalsIgnoreCase(status), thread);
                    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.debug("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.", fileName, firstScn, nextScn, status, sequence);
                        onlineLogFiles.add(logFile);
                    }
                    else {
                        LOGGER.debug("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.", fileName, firstScn, nextScn, status, sequence);
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
                    LOGGER.debug("Removing archive log {} with duplicate sequence {} to {}", f.getFileName(), f.getSequence(), redoLog.getFileName());
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
     * This method removes all added log files from mining
     * @param conn connection
     * @throws SQLException something happened
     */
    public static void removeLogFilesFromMining(OracleConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.connection(false).prepareStatement("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS");
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
