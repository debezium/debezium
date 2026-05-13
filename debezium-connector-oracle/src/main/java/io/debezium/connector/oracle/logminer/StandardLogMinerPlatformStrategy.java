/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;

/**
 * Standard LogMiner platform strategy for on-premise and self-managed Oracle deployments.
 *
 * <p>Uses the standard Oracle {@code SYS.DBMS_LOGMNR} and {@code DBMS_LOGMNR_D} PL/SQL packages
 * and standard {@code V$} views. This is the default strategy when no specific deployment
 * platform is configured.
 *
 * @author Chris Cranford
 */
public class StandardLogMinerPlatformStrategy implements LogMinerPlatformStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardLogMinerPlatformStrategy.class);

    @Override
    public void addLogFile(OracleConnection connection, String fileName) throws SQLException {
        connection.executeWithoutCommitting(
                "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" + fileName + "', OPTIONS => DBMS_LOGMNR.ADDFILE); END;");
    }

    @Override
    public void startSession(OracleConnection connection, Scn startScn, Scn endScn, String miningOptions,
                             String dictionaryFilePath)
            throws SQLException {
        final var query = new StringBuilder(64);
        query.append("BEGIN sys.dbms_logmnr.start_logmnr(");
        if (!startScn.isNull()) {
            query.append("startScn => '").append(startScn).append("', ");
        }
        if (!endScn.isNull()) {
            query.append("endScn => '").append(endScn).append("', ");
        }
        query.append("options => ").append(miningOptions);
        if (dictionaryFilePath != null) {
            query.append(", DICTFILENAME => '").append(dictionaryFilePath).append("'");
        }
        query.append("); END;");
        connection.executeWithoutCommitting(query.toString());
    }

    @Override
    public void endSession(OracleConnection connection) throws SQLException {
        connection.executeWithoutCommitting("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
    }

    @Override
    public void writeDataDictionaryToRedoLogs(OracleConnection connection) throws SQLException {
        connection.executeWithoutCommitting("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
    }

    @Override
    public void removeLogFile(OracleConnection connection, String fileName) throws SQLException {
        LOGGER.debug("Removing file {} from LogMiner mining session.", fileName);
        final String sql = "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
        try (CallableStatement statement = connection.connection(false).prepareCall(sql)) {
            statement.execute();
        }
    }

    @Override
    public boolean isCdbRootAccessible() {
        return true;
    }

    @Override
    public String getCurrentScnQuery() {
        return "SELECT CURRENT_SCN FROM V$DATABASE";
    }

    @Override
    public String getArchiveLogModeQuery() {
        return "SELECT LOG_MODE FROM V$DATABASE";
    }

    @Override
    public String getRedoThreadStateQuery() {
        return "SELECT * FROM V$THREAD";
    }

    /**
     * Get the set of log file names currently registered in the LogMiner session.
     *
     * @param connection the Oracle database connection
     * @return set of log file names
     * @throws SQLException if a database exception occurs
     */
    public static Set<String> getRegisteredLogFileNames(OracleConnection connection) throws SQLException {
        return connection.queryAndMap("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS", rs -> {
            final var results = new java.util.HashSet<String>();
            while (rs.next()) {
                results.add(rs.getString(1));
            }
            return results;
        });
    }
}
