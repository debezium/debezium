/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerSessionContext;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Clock;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Calculates the resume position for unbuffered LogMiner sessions on a periodic interval.
 * <p>
 * The main LogMiner session used by the unbuffered implementation uses {@code COMMITTED_DATA_ONLY} mode,
 * which means that LogMiner only returns data that has been committed in the transaction logs, excluding
 * things that have been rolled back or not yet committed.
 * <p>
 * This class uses the traditional "uncommitted" mode, gathers all start, commit, and rollback markers,
 * and determines the eldest transaction that is still in flight between the current resume position and
 * the current latest commit position.
 *
 * @author Chris Cranford
 */
public class ResumePositionProvider implements AutoCloseable {

    private final Logger LOGGER = LoggerFactory.getLogger(ResumePositionProvider.class);

    private final OracleConnectorConfig connectorConfig;
    private final JdbcConfiguration jdbcConfig;
    private final Duration updateInterval;

    private OracleConnection connection;
    private LogMinerSessionContext sessionContext;
    private volatile Timer queryTimer;

    public ResumePositionProvider(OracleConnectorConfig connectorConfig, JdbcConfiguration jdbcConfig) {
        this.connectorConfig = connectorConfig;
        this.jdbcConfig = jdbcConfig;
        this.updateInterval = connectorConfig.getResumePositionUpdateInterval();
        this.queryTimer = resetTimer();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            LOGGER.info("Stopping the unbuffered resume position provider");
            sessionContext.close();
            connection.close();
        }
    }

    public Scn computeResumePositionFromLogs(Scn currentResumeScn, Scn currentCommitScn, List<LogFile> logFiles) throws SQLException {
        if (!queryTimer.expired()) {
            return currentResumeScn;
        }

        try {

            // Lazily create the connection if it doesn't exist.
            if (connection == null) {
                LOGGER.info("Starting the unbuffered resume position provider");
                connection = new OracleConnection(jdbcConfig, false);

                // Always make sure connection is not set to auto-commit
                connection.setAutoCommit(false);

                // LogMiner must be run in the CDB
                if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
                    connection.resetSessionToCdb();
                }

                sessionContext = new LogMinerSessionContext(connection, false, LogMiningStrategy.ONLINE_CATALOG);
            }

            sessionContext.removeAllLogFilesFromSession();

            sessionContext.addLogFiles(logFiles);

            sessionContext.startSession(currentResumeScn, Scn.NULL, false, null);

            final Scn resumeScn = connection.prepareQueryAndMap(
                    "SELECT * FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE IN (6,7,36) AND SCN <= ?",
                    ps -> ps.setString(1, currentCommitScn.toString()),
                    rs -> {
                        final Map<String, Transaction> transactions = new LinkedHashMap<>();
                        while (rs.next()) {
                            final byte[] xid = rs.getBytes("XID");
                            final String transactionId = xid == null ? null : HexConverter.convertToHexString(xid);
                            if (transactionId != null) {
                                final EventType eventType = EventType.from(rs.getInt("OPERATION_CODE"));
                                final Scn scn = Scn.valueOf(rs.getString("SCN"));
                                if (EventType.START == eventType) {
                                    transactions.put(transactionId, new Transaction(scn));
                                }
                                else {
                                    final Transaction transaction = transactions.get(transactionId);
                                    if (transaction == null) {
                                        LOGGER.trace(
                                                "Ignoring transaction {} event {} at SCN {} as it must have started before current resume SCN {}.",
                                                transactionId, eventType, scn, currentResumeScn);
                                    }
                                    else {
                                        transaction.markEnded(scn);
                                    }
                                }
                            }
                        }

                        return transactions.values().stream()
                                .filter(Transaction::isInProgress)
                                .findFirst()
                                .map(Transaction::getStartScn)
                                .orElse(currentCommitScn);
                    });

            LOGGER.debug("Resume/Commit SCN {}/{} - new resume SCN is {}", currentResumeScn, currentCommitScn, resumeScn);
            return resumeScn;
        }
        finally {
            sessionContext.endMiningSession();
            queryTimer = resetTimer();
        }
    }

    private Timer resetTimer() {
        return Threads.timer(Clock.SYSTEM, updateInterval);
    }

    private static class Transaction {
        private final Scn startScn;
        private Scn endScn;

        Transaction(Scn startScn) {
            this.startScn = startScn;
        }

        public void markEnded(Scn scn) {
            this.endScn = scn;
        }

        public boolean isInProgress() {
            return this.endScn == null;
        }

        public Scn getStartScn() {
            return startScn;
        }
    }
}
