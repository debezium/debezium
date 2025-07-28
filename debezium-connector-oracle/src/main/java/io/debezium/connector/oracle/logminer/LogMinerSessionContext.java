/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.Scn;

/**
 * A context class that provides centralized control over an Oracle LogMiner session.
 *
 * @author Chris Cranford
 */
public class LogMinerSessionContext implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSessionContext.class);

    private final OracleConnection connection;
    private final boolean useContinuousMining;
    private final LogMiningStrategy strategy;
    private final String dictionaryFilePath;

    private boolean sessionStarted = false;
    private Duration lastSessionStartTime = Duration.ZERO;
    private Scn currentSessionStartScn = Scn.NULL;
    private Scn currentSessionEndScn = Scn.NULL;

    public LogMinerSessionContext(OracleConnection connection, boolean useContinuousMining, LogMiningStrategy strategy, String dictionaryFilePath) {
        this.connection = connection;
        this.useContinuousMining = useContinuousMining;
        this.strategy = strategy;
        this.dictionaryFilePath = dictionaryFilePath;
    }

    @Override
    public void close() throws Exception {
        endMiningSession();
    }

    /**
     * Get the current session starting system change number.
     *
     * @return the current starting system change number or {@link Scn#NULL} if not started
     */
    public Scn getCurrentSessionStartScn() {
        return currentSessionStartScn;
    }

    /**
     * Get the current session ending system change number.
     *
     * @return the current ending system change number or {@link Scn#NULL} if not started
     */
    public Scn getCurrentSessionEndScn() {
        return currentSessionEndScn;
    }

    /**
     * Get how long it took for the last mining session to start.
     *
     * @return the duration to start the last call to {@link #startSession(Scn, Scn, boolean)}
     */
    public Duration getLastSessionStartTime() {
        return lastSessionStartTime;
    }

    /**
     * Check whether a mining session has already started.
     *
     * @return {@code true} if the mining session is started, {@code false} otherwise
     */
    public boolean isSessionStarted() {
        return sessionStarted;
    }

    /**
     * Add the log file to the LogMiner session.
     *
     * @param logFileName the log file to add to the session, should not be {@code null}
     * @throws SQLException if a database exception occurred registering the log file
     */
    public void addLogFile(String logFileName) throws SQLException {
        Objects.requireNonNull(logFileName);

        LOGGER.trace("Adding log file '{}' to the mining session.", logFileName);
        connection.executeWithoutCommitting("BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" +
                logFileName + "', OPTIONS => DBMS_LOGMNR.ADDFILE); END;");
    }

    /**
     * Adds all the given logs to the session.
     *
     * @param logFiles collection of log files
     * @throws SQLException if a database exception occurred registering the log files
     */
    public void addLogFiles(List<LogFile> logFiles) throws SQLException {
        for (LogFile logFile : logFiles) {
            addLogFile(logFile.getFileName());
        }
    }

    /**
     * Removes all log files from the mining session.
     *
     * @throws SQLException if a database exception occurred
     */
    public void removeAllLogFilesFromSession() throws SQLException {
        connection.removeAllLogFilesFromLogMinerSession();
    }

    /**
     * Starts the LogMiner session. All logs should have been registered prior to this call.
     *
     * @param startScn starting system change number, may be {@link Scn#NULL} to leave unset
     * @param endScn ending system change number, may be {@link Scn#NULL} to leave unset
     * @param committedDataOnly whether to use committed data only mode
     * @throws SQLException if a database exception occurred starting the mining session
     * @throws RetriableLogMinerException if the operation should be retried
     */
    public void startSession(Scn startScn, Scn endScn, boolean committedDataOnly) throws SQLException {
        Objects.requireNonNull(startScn, "The start SCN must be provided, but can be Scn.NULL");
        Objects.requireNonNull(endScn, "The end SCN must be provided, but can be Scn.NULL");

        try {
            final StringBuilder query = new StringBuilder(64);
            query.append("BEGIN sys.dbms_logmnr.start_logmnr(");
            if (!startScn.isNull()) {
                query.append("startScn => '").append(startScn).append("', ");
            }
            if (!endScn.isNull()) {
                query.append("endScn => '").append(endScn).append("', ");
            }
            query.append("options => ").append(String.join(" + ", getMiningOptions(committedDataOnly)));
            if (strategy == OracleConnectorConfig.LogMiningStrategy.DICTIONARY_FROM_FILE) {
                query.append(", DICTFILENAME => '").append(dictionaryFilePath).append("'");
            }
            query.append("); END;");

            Instant startTime = Instant.now();
            connection.executeWithoutCommitting(query.toString());
            lastSessionStartTime = Duration.between(startTime, Instant.now());
            sessionStarted = true;

            currentSessionStartScn = startScn;
            currentSessionEndScn = endScn;
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 1291 || e.getMessage().startsWith("ORA-01291")) {
                throw new RetriableLogMinerException(e);
            }
            else if (e.getErrorCode() == 310 || e.getMessage().startsWith("ORA-00310")) {
                throw new RetriableLogMinerException(e);
            }
            throw e;
        }
    }

    /**
     * Ends the current mining session.
     *
     * @throws SQLException if a database exception occurs while closing the mining session
     */
    public void endMiningSession() throws SQLException {
        try {
            LOGGER.trace("Ending log mining session");

            currentSessionStartScn = Scn.NULL;
            currentSessionEndScn = Scn.NULL;

            connection.executeWithoutCommitting("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
            sessionStarted = false;
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

    /**
     * Writes the data dictionary to the Oracle transaction logs.
     *
     * @throws SQLException if a database exception occurs
     */
    public void writeDataDictionaryToRedoLogs() throws SQLException {
        LOGGER.trace("Building data dictionary");
        connection.executeWithoutCommitting("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
    }

    private List<String> getMiningOptions(boolean committedDataOnly) {
        final List<String> miningOptions = new ArrayList<>();
        if (strategy.equals(LogMiningStrategy.CATALOG_IN_REDO)) {
            miningOptions.add("DBMS_LOGMNR.DICT_FROM_REDO_LOGS");
            miningOptions.add("DBMS_LOGMNR.DDL_DICT_TRACKING");
        }
        else {
            miningOptions.add("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG");
        }

        if (useContinuousMining) {
            miningOptions.add("DBMS_LOGMNR.CONTINUOUS_MINE");
        }

        if (committedDataOnly) {
            miningOptions.add("DBMS_LOGMNR.COMMITTED_DATA_ONLY");
        }

        miningOptions.add("DBMS_LOGMNR.NO_ROWID_IN_STMT");

        return miningOptions;
    }
}
