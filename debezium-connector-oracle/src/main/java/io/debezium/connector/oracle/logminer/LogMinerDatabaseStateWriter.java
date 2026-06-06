/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;

/**
 * Utility class that can write the contents of several Oracle LogMiner tables to the
 * connector's log if and only if DEBUG logging is enabled.
 *
 * @author Chris Cranford
 */
public class LogMinerDatabaseStateWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerDatabaseStateWriter.class);

    public static void write(OracleConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Configured redo logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGFILE");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain redo log table entries", e);
            }
            LOGGER.debug("Archive logs for last 48 hours:");
            try {
                logQueryResults(connection, "SELECT * FROM V$ARCHIVED_LOG WHERE FIRST_TIME >= SYSDATE - 2");
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
        }
    }

    public static void writeLogMinerStartParameters(OracleConnection connection) {
        final String query = "SELECT START_SCN, END_SCN, REQUIRED_START_SCN, OPTIONS, STATUS, INFO " +
                "FROM V$LOGMNR_PARAMETERS";
        try {
            connection.query(query, rs -> {
                if (rs.next()) {
                    LOGGER.error("Failed to start a LogMiner mining session with parameters: ");
                    do {
                        LOGGER.error("\tSCN: [{}-{}], Required Start SCN: {}, Options: {}, Status: {} - {}",
                                rs.getString(1),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getString(4),
                                rs.getString(5),
                                rs.getString(6) == null ? "N/A" : rs.getString(6));
                    } while (rs.next());
                }
            });
        }
        catch (SQLException e) {
            // No need to throw this error as this method should only be called due to an earlier
            // error, and this is intended to provide details regarding the original error.
            LOGGER.error("Failed to read contents of V$LOGMNR_PARAMETERS", e);
        }
    }

    public static void writeLogMinerLogFailures(OracleConnection connection) {
        // Query fetches all logs that had problems when LogMiner started.
        final String query = "SELECT FILENAME, THREAD_ID, THREAD_SQN, LOW_SCN, NEXT_SCN, DICTIONARY_BEGIN, " +
                "DICTIONARY_END, TYPE, INFO FROM V$LOGMNR_LOGS ORDER BY THREAD_ID, THREAD_SQN";
        try {
            connection.query(query, rs -> {
                if (rs.next()) {
                    final String dictionaryBegin = rs.getString(6);
                    final String dictionaryEnd = rs.getString(7);

                    final String dictionaryStatus;
                    if ("YES".equals(dictionaryBegin)) {
                        dictionaryStatus = "YES".equals(dictionaryEnd) ? "BEGIN+END" : "BEGIN";
                    }
                    else {
                        dictionaryStatus = "YES".equals(dictionaryEnd) ? "END" : "NONE";
                    }

                    LOGGER.error("The following logs triggered a LogMiner failure:");
                    do {
                        LOGGER.error("\t* File '{}', Thread {} (Seq {}), SCN [{} - {}], Type {}, Dictionary {}: {}",
                                rs.getString(1),
                                rs.getLong(2),
                                rs.getLong(3),
                                rs.getString(4),
                                rs.getString(5),
                                rs.getString(8),
                                dictionaryStatus,
                                rs.getString(9));
                    } while (rs.next());
                }
                else {
                    LOGGER.error("No rows found in V$LOGMNR_LOGS");
                }
            });
        }
        catch (SQLException e) {
            // No need to throw this error as this method should only be called due to an earlier
            // error, and this is intended to provide details regarding the original error.
            LOGGER.error("Failed to read contents of V$LOGMNR_LOGS to determine log failures", e);
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

}
