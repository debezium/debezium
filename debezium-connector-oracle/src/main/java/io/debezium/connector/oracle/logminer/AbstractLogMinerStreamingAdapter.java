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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.oracle.AbstractStreamingAdapter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.OracleConnectorConfig.TransactionSnapshotBoundaryMode;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.document.Document;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

/**
 * An abstract base class for LogMiner streaming adapters.
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerStreamingAdapter
        extends AbstractStreamingAdapter<LogMinerStreamingChangeEventSourceMetrics> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLogMinerStreamingAdapter.class);

    public AbstractLogMinerStreamingAdapter(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
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
    public LogMinerStreamingChangeEventSourceMetrics getStreamingMetrics(OracleTaskContext taskContext,
                                                                         ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                         EventMetadataProvider metadataProvider,
                                                                         OracleConnectorConfig connectorConfig) {
        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, metadataProvider, connectorConfig);
    }

    @Override
    public OracleOffsetContext determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                                       OracleConnectorConfig connectorConfig,
                                                       OracleConnection connection)
            throws SQLException {

        final Scn latestTableDdlScn = getLatestTableDdlScn(ctx, connection).orElse(null);
        final String tableName = getTransactionTableName(connectorConfig);

        final Map<String, Scn> pendingTransactions = new LinkedHashMap<>();

        final Optional<Scn> currentScn;
        if (isPendingTransactionSkip(connectorConfig)) {
            currentScn = getCurrentScn(latestTableDdlScn, connection);
        }
        else {
            currentScn = getPendingTransactions(latestTableDdlScn, connection, pendingTransactions, tableName);
        }

        if (currentScn.isEmpty()) {
            throw new DebeziumException("Failed to resolve current SCN");
        }

        // The provided snapshot connection already has an in-progress transaction with a save point
        // that prevents switching from a PDB to the root CDB and if invoking the LogMiner APIs on
        // such a connection, the use of commit/rollback by LogMiner will drop/invalidate the save
        // point as well. A separate connection is necessary to preserve the save point.
        try (OracleConnection conn = new OracleConnection(connection.config(), false)) {
            conn.setAutoCommit(false);
            if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
                // The next stage cannot be run within the PDB, reset the connection to the CDB.
                conn.resetSessionToCdb();
            }
            return determineSnapshotOffset(connectorConfig, conn, currentScn.get(), pendingTransactions, tableName);
        }
    }

    @Override
    public Scn getOffsetScn(OracleOffsetContext offsetContext) {
        return offsetContext.getScn();
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
                                                 Map<String, Scn> transactions, String transactionTableName)
            throws SQLException {
        final String query = "SELECT d.CURRENT_SCN, t.XID, t.START_SCN "
                + "FROM V$DATABASE d "
                + "LEFT OUTER JOIN " + transactionTableName + " t "
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
                        final String transactionId = HexConverter.convertToHexString(rs.getBytes(2));
                        final Scn transactionStartScn = Scn.valueOf(pendingTxStartScn);
                        // There is a use case where if the archive logs do not contain sufficient logs where the
                        // transaction started, LogMiner will return a value of 0 as the START_SCN and this can
                        // unintentionally cause starting from the beginning of time.
                        if (transactionStartScn.compareTo(Scn.ONE) > 0) {
                            // There is a pending transaction, capture state
                            transactions.put(transactionId, transactionStartScn);
                        }
                        else {
                            LOGGER.warn("Unable to determine the start SCN, transaction {} will not be included", transactionId);
                        }
                    }
                }
            }
            catch (SQLException e) {
                LOGGER.warn("Could not query the {} view: {}", transactionTableName, e.getMessage(), e);
                throw e;
            }

        } while (areSameTimestamp(latestTableDdlScn, currentScn, connection));

        for (Map.Entry<String, Scn> transaction : transactions.entrySet()) {
            LOGGER.trace("\tPending Transaction '{}' started at SCN {}", transaction.getKey(), transaction.getValue());
        }

        return Optional.ofNullable(currentScn);
    }

    private OracleOffsetContext determineSnapshotOffset(OracleConnectorConfig connectorConfig,
                                                        OracleConnection connection,
                                                        Scn currentScn,
                                                        Map<String, Scn> pendingTransactions,
                                                        String transactionTableName)
            throws SQLException {

        if (isPendingTransactionSkip(connectorConfig)) {
            LOGGER.info("\tNo in-progress transactions will be captured.");
        }
        else if (isPendingTransactionViewOnly(connectorConfig)) {
            LOGGER.info("\tSkipping transaction logs for resolving snapshot offset, only using {}.", transactionTableName);
        }
        else {
            LOGGER.info("\tConsulting {} and transaction logs for resolving snapshot offset.", transactionTableName);
            getPendingTransactionsFromLogs(connection, currentScn, pendingTransactions);
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

    protected Scn getOldestScnAvailableInLogs(OracleConnectorConfig config, OracleConnection connection) throws SQLException {
        final Duration archiveLogRetention = config.getArchiveLogRetention();
        final String archiveLogDestinationName = config.getArchiveLogDestinationName();
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

    protected List<LogFile> getOrderedLogsFromScn(OracleConnectorConfig config, Scn sinceScn, OracleConnection connection) throws SQLException {
        final LogFileCollector collector = new LogFileCollector(config, connection);
        return collector.getLogs(sinceScn)
                .stream()
                .sorted(Comparator.comparing(LogFile::getSequence))
                .collect(Collectors.toList());
    }

    protected void getPendingTransactionsFromLogs(OracleConnection connection, Scn currentScn, Map<String, Scn> pendingTransactions) throws SQLException {
        final Scn oldestScn = getOldestScnAvailableInLogs(connectorConfig, connection);
        final List<LogFile> logFiles = getOrderedLogsFromScn(connectorConfig, oldestScn, connection);
        if (!logFiles.isEmpty()) {
            try (var context = new LogMinerSessionContext(connection, false, LogMiningStrategy.ONLINE_CATALOG, connectorConfig.getLogMiningPathToDictionary())) {
                context.addLogFiles(getMostRecentLogFilesForSearch(logFiles));
                context.startSession(Scn.NULL, Scn.NULL, false);

                LOGGER.info("\tQuerying transaction logs, please wait...");
                connection.query("SELECT START_SCN, XID FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE=7 AND SCN >= " + currentScn + " AND START_SCN <= " + currentScn,
                        rs -> {
                            while (rs.next()) {
                                final String transactionId = HexConverter.convertToHexString(rs.getBytes("XID"));
                                final String startScnStr = rs.getString("START_SCN");
                                if (!Strings.isNullOrBlank(startScnStr)) {
                                    final Scn startScn = Scn.valueOf(rs.getString("START_SCN"));
                                    if (!pendingTransactions.containsKey(transactionId)) {
                                        LOGGER.info("\tTransaction '{}' started at SCN '{}'", transactionId, startScn);
                                        pendingTransactions.put(transactionId, startScn);
                                    }
                                }
                            }
                        });
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to resolve snapshot offset", e);
            }
        }
    }

    protected List<LogFile> getMostRecentLogFilesForSearch(List<LogFile> allLogFiles) {
        Map<Integer, List<LogFile>> recentLogsPerThread = new HashMap<>();
        for (LogFile logFile : allLogFiles) {
            if (!recentLogsPerThread.containsKey(logFile.getThread())) {
                if (logFile.isCurrent()) {
                    recentLogsPerThread.put(logFile.getThread(), new ArrayList<>());
                    recentLogsPerThread.get(logFile.getThread()).add(logFile);
                    final Optional<LogFile> maxArchiveLogFile = allLogFiles.stream()
                            .filter(f -> logFile.getThread() == f.getThread() && logFile.getSequence().compareTo(f.getSequence()) > 0)
                            .max(Comparator.comparing(LogFile::getSequence));
                    maxArchiveLogFile.ifPresent(file -> recentLogsPerThread.get(logFile.getThread()).add(file));
                }
            }
        }

        final List<LogFile> logs = new ArrayList<>();
        for (Map.Entry<Integer, List<LogFile>> entry : recentLogsPerThread.entrySet()) {
            logs.addAll(entry.getValue());
        }
        return logs;
    }

    private boolean isPendingTransactionSkip(OracleConnectorConfig config) {
        return config.getLogMiningTransactionSnapshotBoundaryMode() == TransactionSnapshotBoundaryMode.SKIP;
    }

    public boolean isPendingTransactionViewOnly(OracleConnectorConfig config) {
        return config.getLogMiningTransactionSnapshotBoundaryMode() == TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY;
    }

    /**
     * Under Oracle RAC, the V$ tables are specific the node that the JDBC connection is established to and
     * not every V$ is synchronized across the cluster.  Therefore, when Oracle RAC is in play, we should
     * use the GV$ tables instead.
     *
     * @param config the connector configuration, should not be {@code null}
     * @return the pending transaction table name
     */
    private static String getTransactionTableName(OracleConnectorConfig config) {
        if (config.getRacNodes() == null || config.getRacNodes().isEmpty()) {
            return "V$TRANSACTION";
        }
        return "GV$TRANSACTION";
    }
}
