/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.AbstractStreamingAdapter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.TransactionSnapshotBoundaryMode;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;
import io.debezium.util.HexConverter;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
public class LogMinerAdapter extends AbstractStreamingAdapter {

    private static final Duration GET_TRANSACTION_SCN_PAUSE = Duration.ofSeconds(1);

    private static final int GET_TRANSACTION_SCN_ATTEMPTS = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerAdapter.class);

    public static final String TYPE = "logminer";

    public LogMinerAdapter(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return resolveScn(recorded).compareTo(resolveScn(desired)) < 1;
            }
        };
    }

    @Override
    public OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader() {
        return new LogMinerOracleOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<OraclePartition, OracleOffsetContext> getSource(OracleConnection connection,
                                                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                                      ErrorHandler errorHandler,
                                                                                      Clock clock,
                                                                                      OracleDatabaseSchema schema,
                                                                                      OracleTaskContext taskContext,
                                                                                      Configuration jdbcConfig,
                                                                                      OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        return new LogMinerStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                jdbcConfig,
                streamingMetrics);
    }

    @Override
    public OracleOffsetContext determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                                       OracleConnectorConfig connectorConfig,
                                                       OracleConnection connection)
            throws SQLException {

        final Scn latestTableDdlScn = getLatestTableDdlScn(ctx, connection).orElse(null);

        final Map<String, Scn> pendingTransactions = new LinkedHashMap<>();

        final Optional<Scn> currentScn;
        if (isPendingTransactionSkip(connectorConfig)) {
            currentScn = getCurrentScn(latestTableDdlScn, connection);
        }
        else {
            currentScn = getPendingTransactions(latestTableDdlScn, connection, pendingTransactions);
        }

        if (!currentScn.isPresent()) {
            throw new DebeziumException("Failed to resolve current SCN");
        }

        // The provided snapshot connection already has an in-progress transaction with a save point
        // that prevents switching from a PDB to the root CDB and if invoking the LogMiner APIs on
        // such a connection, the use of commit/rollback by LogMiner will drop/invalidate the save
        // point as well. A separate connection is necessary to preserve the save point.
        try (OracleConnection conn = new OracleConnection(connection.config(), () -> getClass().getClassLoader(), false)) {
            conn.setAutoCommit(false);
            if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
                // The next stage cannot be run within the PDB, reset the connection to the CDB.
                conn.resetSessionToCdb();
            }
            return determineSnapshotOffset(connectorConfig, conn, currentScn.get(), pendingTransactions);
        }
    }

    private Optional<Scn> getCurrentScn(Scn latestTableDdlScn, OracleConnection connection) throws SQLException {
        final String query = "SELECT CURRENT_SCN FROM V$DATABASE";

        Scn currentScn;
        do {
            currentScn = connection.queryAndMap(query, rs -> rs.next() ? Scn.valueOf(rs.getString(1)) : Scn.NULL);
        } while (areSameTimestamp(latestTableDdlScn, currentScn, connection));

        return Optional.ofNullable(currentScn);
    }

    private Optional<Scn> getPendingTransactions(Scn latestTableDdlScn, OracleConnection connection,
                                                 Map<String, Scn> transactions)
            throws SQLException {
        final String query = "SELECT d.CURRENT_SCN, t.XID, t.START_SCN "
                + "FROM V$DATABASE d "
                + "LEFT OUTER JOIN V$TRANSACTION t "
                + "ON t.START_SCN < d.CURRENT_SCN ";

        Scn currentScn = null;
        do {
            // Clear iterative state
            currentScn = null;
            transactions.clear();

            try (Statement s = connection.connection().createStatement(); ResultSet rs = s.executeQuery(query)) {
                while (rs.next()) {
                    if (currentScn == null) {
                        // Only need to set this once per iteration
                        currentScn = Scn.valueOf(rs.getString(1));
                    }
                    final String pendingTxStartScn = rs.getString(3);
                    if (!Strings.isNullOrEmpty(pendingTxStartScn)) {
                        // There is a pending transaction, capture state
                        transactions.put(HexConverter.convertToHexString(rs.getBytes(2)), Scn.valueOf(pendingTxStartScn));
                    }
                }
            }
            catch (SQLException e) {
                LOGGER.warn("Could not query the V$TRANSACTION view: {}", e.getMessage(), e);
                throw e;
            }

        } while (areSameTimestamp(latestTableDdlScn, currentScn, connection));

        return Optional.ofNullable(currentScn);
    }

    private OracleOffsetContext determineSnapshotOffset(OracleConnectorConfig connectorConfig,
                                                        OracleConnection connection,
                                                        Scn currentScn,
                                                        Map<String, Scn> pendingTransactions)
            throws SQLException {

        if (isPendingTransactionSkip(connectorConfig)) {
            LOGGER.info("\tNo in-progress transactions will be captured.");
        }
        else if (isPendingTransactionViewOnly(connectorConfig)) {
            LOGGER.info("\tSkipping transaction logs for resolving snapshot offset, only using V$TRANSACTION.");
        }
        else {
            LOGGER.info("\tConsulting V$TRANSACTION and transaction logs for resolving snapshot offset.");
            final Scn oldestScn = getOldestScnAvailableInLogs(connectorConfig, connection);
            final List<LogFile> logFiles = getOrderedLogsFromScn(connectorConfig, oldestScn, connection);

            // Simple sanity check
            // While this should never be the case, this is to guard against NPE or other errors that could
            // result from below if there is some race condition/corner case not considered
            if (logFiles.isEmpty()) {
                throw new DebeziumException("Failed to get log files from Oracle");
            }

            // Locate the index in the log files where we should begin
            // This is the log where the current SCN exists
            int logIndex = 0;
            for (int i = 0; i < logFiles.size(); ++i) {
                if (logFiles.get(i).isScnInLogFileRange(currentScn)) {
                    logIndex = i;
                    break;
                }
            }

            // Starting from the log position (logIndex), we begin mining from it going backward.
            // Each iteration will include the prior log along with the logs up to the logIndex to locate the start pos
            for (int pos = logIndex; pos >= 0; pos--) {
                try {
                    addLogsToSession(logFiles, pos, logIndex, connection);
                    startSession(connection);

                    final Optional<String> transactionId = getTransactionIdForScn(currentScn, connection);
                    if (!transactionId.isPresent()) {
                        throw new DebeziumException("Failed to get transaction id for current SCN " + currentScn);
                    }

                    if (pendingTransactions.containsKey(transactionId.get())) {
                        // The transaction was already captured in the pending transactions list.
                        // There is nothing special to do here, it is safe to end the session
                        LOGGER.info("\tCurrent SCN transaction '{}' was found in V$TRANSACTION", transactionId.get());
                        break;
                    }

                    // The current SCN transaction is not in the pending transaction list.
                    // We need to attempt to fully mine the transaction to see how to handle it.
                    Scn startScn = getTransactionStartScn(transactionId.get(), currentScn, connection);
                    if (startScn.isNull() && pos == 0) {
                        LOGGER.warn("Failed to find start SCN for transaction '{}', transaction will not be included.",
                                transactionId.get());
                    }
                    else {
                        pendingTransactions.put(transactionId.get(), startScn);
                        break;
                    }
                }
                catch (InterruptedException e) {
                    throw new DebeziumException("Failed to resolve snapshot offset", e);
                }
                finally {
                    stopSession(connection);
                }
            }
        }

        if (!pendingTransactions.isEmpty()) {
            for (Map.Entry<String, Scn> entry : pendingTransactions.entrySet()) {
                LOGGER.info("\tFound in-progress transaction {}, starting at SCN {}", entry.getKey(), entry.getValue());
            }
        }
        else if (!isPendingTransactionSkip(connectorConfig)) {
            LOGGER.info("\tFound no in-progress transactions.");
        }

        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(currentScn)
                .snapshotScn(currentScn)
                .snapshotPendingTransactions(pendingTransactions)
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }

    private void addLogsToSession(List<LogFile> logs, int from, int to, OracleConnection connection) throws SQLException {
        for (int i = from; i <= to; ++i) {
            final LogFile logFile = logs.get(i);
            LOGGER.debug("\tAdding log: {}", logFile.getFileName());
            connection.executeWithoutCommitting(SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", logFile.getFileName()));
        }
    }

    private void startSession(OracleConnection connection) throws SQLException {
        // We explicitly use the ONLINE data dictionary mode here.
        // Since we are only concerned about non-SQL columns, it is safe to always use this mode
        final String query = "BEGIN sys.dbms_logmnr.start_logmnr("
                + "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.NO_ROWID_IN_STMT);"
                + "END;";
        LOGGER.debug("\tStarting mining session");
        connection.executeWithoutCommitting(query);
    }

    private void stopSession(OracleConnection connection) throws SQLException {
        // stop the current mining session
        try {
            LOGGER.debug("\tStopping mining session");
            connection.executeWithoutCommitting("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
        }
        catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.debug("LogMiner mining session is already closed.");
            }
            else {
                throw e;
            }
        }
    }

    private Scn getOldestScnAvailableInLogs(OracleConnectorConfig config, OracleConnection connection) throws SQLException {
        final Duration archiveLogRetention = config.getLogMiningArchiveLogRetention();
        final String archiveLogDestinationName = config.getLogMiningArchiveDestinationName();
        return connection.queryAndMap(SqlUtils.oldestFirstChangeQuery(archiveLogRetention, archiveLogDestinationName),
                rs -> {
                    if (rs.next()) {
                        final String value = rs.getString(1);
                        if (!Strings.isNullOrEmpty(value)) {
                            return Scn.valueOf(value);
                        }
                    }
                    return Scn.NULL;
                });
    }

    private List<LogFile> getOrderedLogsFromScn(OracleConnectorConfig config, Scn sinceScn, OracleConnection connection) throws SQLException {
        return LogMinerHelper.getLogFilesForOffsetScn(connection, sinceScn, config.getLogMiningArchiveLogRetention(),
                config.isArchiveLogOnlyMode(), config.getLogMiningArchiveDestinationName())
                .stream()
                .sorted(Comparator.comparing(LogFile::getSequence))
                .collect(Collectors.toList());
    }

    private Optional<String> getTransactionIdForScn(Scn scn, OracleConnection connection) throws SQLException, InterruptedException {
        LOGGER.debug("\tGet transaction id for SCN {}", scn);
        final AtomicReference<String> transactionId = new AtomicReference<>();
        for (int attempt = 1; attempt <= GET_TRANSACTION_SCN_ATTEMPTS; ++attempt) {
            connection.call("SELECT XID FROM V$LOGMNR_CONTENTS WHERE SCN = ?",
                    s -> s.setLong(1, scn.longValue()),
                    rs -> {
                        if (rs.next()) {
                            transactionId.set(HexConverter.convertToHexString(rs.getBytes("XID")));
                        }
                    });
            if (!Strings.isNullOrEmpty(transactionId.get())) {
                break;
            }
            LOGGER.debug("\tFailed to find transaction for SCN {}, trying again.", scn);
            Metronome.sleeper(GET_TRANSACTION_SCN_PAUSE, Clock.SYSTEM).pause();
        }
        return Optional.ofNullable(transactionId.get());
    }

    private Scn getTransactionStartScn(String transactionId, Scn currentScn, OracleConnection connection) throws SQLException, InterruptedException {
        LOGGER.debug("\tGet start SCN for transaction '{}'", transactionId);
        // We perform this operation a maximum of 5 times before we fail.
        final AtomicReference<Scn> startScn = new AtomicReference<>(Scn.NULL);
        for (int attempt = 1; attempt <= GET_TRANSACTION_SCN_ATTEMPTS; ++attempt) {
            connection.call("SELECT SCN, START_SCN, OPERATION FROM V$LOGMNR_CONTENTS WHERE XID=HEXTORAW(UPPER(?))",
                    s -> s.setString(1, transactionId),
                    rs -> {
                        while (rs.next()) {
                            if (!Strings.isNullOrEmpty(rs.getString("START_SCN"))) {
                                final Scn value = Scn.valueOf(rs.getString("START_SCN"));
                                if (currentScn.compareTo(value) == 0) {
                                    startScn.set(value.subtract(Scn.ONE));
                                    LOGGER.debug("\tCurrent SCN {} starts a transaction, using value-1.", value);
                                    break;
                                }
                                startScn.set(Scn.valueOf(rs.getString("START_SCN")));
                                LOGGER.debug("\tCurrent SCN transaction starts at SCN {}", value);
                                break;
                            }
                        }
                    });
            if (!startScn.get().isNull()) {
                break;
            }
            Metronome.sleeper(GET_TRANSACTION_SCN_PAUSE, Clock.SYSTEM).pause();
        }
        return startScn.get();
    }

    private boolean isPendingTransactionSkip(OracleConnectorConfig config) {
        return config.getLogMiningTransactionSnapshotBoundaryMode() == TransactionSnapshotBoundaryMode.SKIP;
    }

    public boolean isPendingTransactionViewOnly(OracleConnectorConfig config) {
        return config.getLogMiningTransactionSnapshotBoundaryMode() == TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY;
    }
}
