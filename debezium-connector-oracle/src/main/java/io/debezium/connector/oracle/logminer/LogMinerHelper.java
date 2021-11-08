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
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
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

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}", lastProcessedScn, logFilesNames);
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
