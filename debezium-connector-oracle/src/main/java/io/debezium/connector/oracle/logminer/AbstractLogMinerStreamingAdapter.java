/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.oracle.AbstractStreamingAdapter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.document.Document;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.history.HistoryRecordComparator;

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
                                                                         OracleConnectorConfig connectorConfig,
                                                                         CapturedTablesSupplier capturedTablesSupplier) {
        return new LogMinerStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, metadataProvider, connectorConfig, capturedTablesSupplier);
    }

    /**
     * Resolves the snapshot offset so that transactions in progress at the snapshot boundary are
     * neither lost nor emitted twice:
     * <ol>
     * <li>Read the current SCN as {@code S0}.</li>
     * <li>Read the oldest start SCN of the transactions in progress as {@code M}.</li>
     * <li>Read the current SCN again as {@code S}, the SCN the snapshot is taken at.</li>
     * </ol>
     * Mining starts at {@code LEAST(S0, M)}, stored as the snapshot SCN. A transaction that commits
     * after {@code S} and started before {@code S0} was still in progress when {@code M} was read,
     * so its start is mined. A transaction that commits at or before {@code S}, stored as the
     * snapshot commit SCN, is already part of the snapshot and is discarded by streaming.
     */
    @Override
    public OracleOffsetContext determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                                       OracleConnectorConfig connectorConfig,
                                                       OracleConnection connection)
            throws SQLException {

        final Scn latestTableDdlScn = getLatestTableDdlScn(ctx, connection).orElse(null);
        final String transactionTableName = getTransactionTableName(connectorConfig);

        final Scn initialScn = getCurrentScn(latestTableDdlScn, connection);
        final Optional<Scn> pendingTransactionStartScn = getMinimumPendingTransactionStartScn(connection, transactionTableName);
        final Scn snapshotCommitScn = getCurrentScn(latestTableDdlScn, connection);

        final Scn snapshotScn = pendingTransactionStartScn
                .filter(startScn -> startScn.compareTo(initialScn) < 0)
                .orElse(initialScn);

        if (pendingTransactionStartScn.isEmpty()) {
            LOGGER.info("\tFound no in-progress transactions.");
        }
        else if (snapshotScn.equals(pendingTransactionStartScn.get())) {
            LOGGER.info("\tOldest in-progress transaction started at SCN {}.", snapshotScn);
        }
        else {
            LOGGER.info("\tOldest in-progress transaction started at SCN {}, after the initial SCN {}.",
                    pendingTransactionStartScn.get(), snapshotScn);
        }
        LOGGER.info("\tSnapshot boundary resolved, mining starts at snapshot SCN {} and the snapshot is taken at snapshot commit SCN {}.",
                snapshotScn, snapshotCommitScn);

        // During the snapshot, the offset SCN is the SCN the snapshot is taken at. The first streaming
        // run rewinds it to the snapshot SCN, after which it is the position where mining resumes.
        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(snapshotCommitScn)
                .snapshotScn(snapshotScn)
                .snapshotCommitScn(snapshotCommitScn)
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }

    @Override
    public Scn getOffsetScn(OracleOffsetContext offsetContext) {
        return offsetContext.getScn();
    }

    private Scn getCurrentScn(Scn latestTableDdlScn, OracleConnection connection) throws SQLException {
        final String query = "SELECT CURRENT_SCN FROM V$DATABASE";

        Scn currentScn;
        do {
            currentScn = connection.queryAndMap(query, rs -> rs.next() ? Scn.valueOf(rs.getString(1)) : Scn.NULL);
        } while (areSameTimestamp(latestTableDdlScn, currentScn, connection));

        if (currentScn == null || currentScn.isNull()) {
            throw new DebeziumException("Failed to resolve current SCN");
        }
        return currentScn;
    }

    /**
     * Reads the oldest start SCN of the transactions currently in progress.
     *
     * @param connection the database connection, should not be {@code null}
     * @param transactionTableName the transaction view name, should not be {@code null}
     * @return the oldest known start SCN, or empty if no transaction with a known start SCN is in progress
     */
    private Optional<Scn> getMinimumPendingTransactionStartScn(OracleConnection connection, String transactionTableName)
            throws SQLException {
        // If the archive logs do not contain the redo where a transaction started, Oracle reports a START_SCN
        // of 0 for it, which would unintentionally cause mining to start from the beginning of time. Such
        // transactions are excluded from the minimum and only counted.
        final String query = "SELECT MIN(CASE WHEN START_SCN > 1 THEN START_SCN END), COUNT(CASE WHEN START_SCN <= 1 THEN 1 END) FROM "
                + transactionTableName;

        final Scn startScn;
        try {
            startScn = connection.queryAndMap(query, rs -> {
                if (!rs.next()) {
                    return Scn.NULL;
                }
                final long unknownStartScnCount = rs.getLong(2);
                if (unknownStartScnCount > 0) {
                    LOGGER.warn("Unable to determine the start SCN of {} in-progress transaction(s), they will not be included", unknownStartScnCount);
                }
                final String value = rs.getString(1);
                return value != null ? Scn.valueOf(value) : Scn.NULL;
            });
        }
        catch (SQLException e) {
            LOGGER.warn("Could not query the {} view: {}", transactionTableName, e.getMessage(), e);
            throw e;
        }

        return startScn.isNull() ? Optional.empty() : Optional.of(startScn);
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
