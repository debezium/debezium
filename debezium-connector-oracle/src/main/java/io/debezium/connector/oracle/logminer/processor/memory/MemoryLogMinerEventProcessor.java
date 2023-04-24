/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerQueryBuilder;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.util.Loggings;

/**
 * A {@link LogMinerEventProcessor} that uses the JVM heap to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerEventProcessor extends AbstractLogMinerEventProcessor<MemoryTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryLogMinerEventProcessor.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final OracleStreamingChangeEventSourceMetrics metrics;

    /**
     * Cache of transactions, keyed based on the transaction's unique identifier
     */
    private final Map<String, MemoryTransaction> transactionCache = new HashMap<>();
    /**
     * Cache of processed transactions (committed or rolled back), keyed based on the transaction's unique identifier.
     */
    private final Map<String, Scn> recentlyProcessedTransactionsCache = new HashMap<>();
    private final Set<Scn> schemaChangesCache = new HashSet<>();
    private final Set<String> abandonedTransactionsCache = new HashSet<>();

    public MemoryLogMinerEventProcessor(ChangeEventSourceContext context,
                                        OracleConnectorConfig connectorConfig,
                                        OracleConnection jdbcConnection,
                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                        OraclePartition partition,
                                        OracleOffsetContext offsetContext,
                                        OracleDatabaseSchema schema,
                                        OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics);
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.metrics = metrics;
    }

    @Override
    protected Map<String, MemoryTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    protected MemoryTransaction createTransaction(LogMinerEventRow row) {
        return new MemoryTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        MemoryTransaction transaction = getTransactionCache().get(row.getTransactionId());
        if (transaction == null) {
            if (isTransactionIdWithNoSequence(row.getTransactionId())) {
                // This means that Oracle LogMiner found an event that should be undone but its corresponding
                // undo entry was read in a prior mining session and the transaction's sequence could not be
                // resolved. In this case, lets locate the transaction based solely on XIDUSN and XIDSLT.
                final String transactionPrefix = getTransactionIdPrefix(row.getTransactionId());
                LOGGER.debug("Undo change refers to a transaction that has no explicit sequence, '{}'", row.getTransactionId());
                LOGGER.debug("Checking all transactions with prefix '{}'", transactionPrefix);
                for (String transactionKey : getTransactionCache().keySet()) {
                    if (transactionKey.startsWith(transactionPrefix)) {
                        transaction = getTransactionCache().get(transactionKey);
                        if (transaction != null && transaction.removeEventWithRowId(row.getRowId())) {
                            // We successfully found a transaction with the same XISUSN and XIDSLT and that
                            // transaction included a change for the specified row id.
                            Loggings.logDebugAndTraceRecord(LOGGER, row, "Undo change on table '{}' was applied to transaction '{}'", row.getTableId(), transactionKey);
                            return;
                        }
                    }
                }
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since event with row-id {} was not found", row.getTableId(),
                        row.getRowId());
            }
            else if (!getConfig().isLobEnabled()) {
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since transaction '{}' was not found.", row.getTableId(),
                        row.getTransactionId());
            }
        }
        else {
            if (!transaction.removeEventWithRowId(row.getRowId())) {
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since event with row-id {} was not found.", row.getTableId(),
                        row.getRowId());
            }
        }
    }

    @Override
    public void close() throws Exception {
        // close any resources used here
    }

    @Override
    public void abandonTransactions(Duration retention) throws InterruptedException {
        if (!Duration.ZERO.equals(retention)) {
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(jdbcConnection, retention);
            if (lastScnToAbandonTransactions.isPresent()) {
                Scn thresholdScn = lastScnToAbandonTransactions.get();
                LOGGER.warn("All transactions with SCN <= {} will be abandoned.", thresholdScn);
                Scn smallestScn = getTransactionCacheMinimumScn();
                if (!smallestScn.isNull() && thresholdScn.compareTo(smallestScn) >= 0) {
                    Iterator<Map.Entry<String, MemoryTransaction>> iterator = transactionCache.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, MemoryTransaction> entry = iterator.next();
                        if (entry.getValue().getStartScn().compareTo(thresholdScn) <= 0) {
                            LOGGER.warn("Transaction {} with start SCN {} is being abandoned.",
                                    entry.getKey(),entry.getValue().getStartScn());

                            abandonedTransactionsCache.add(entry.getKey());
                            iterator.remove();

                            metrics.addAbandonedTransactionId(entry.getKey());
                            metrics.setActiveTransactions(transactionCache.size());
                        }
                    }

                    // Update the oldest scn metric are transaction abandonment
                    smallestScn = getTransactionCacheMinimumScn();
                    metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);

                    offsetContext.setScn(thresholdScn);
                }
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
        }
    }

    @Override
    protected boolean isRecentlyProcessed(String transactionId) {
        return recentlyProcessedTransactionsCache.containsKey(transactionId);
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return schemaChangesCache.contains(row.getScn());
    }

    @Override
    protected MemoryTransaction getAndRemoveTransactionFromCache(String transactionId) {
        return getTransactionCache().remove(transactionId);
    }

    @Override
    protected void removeTransactionAndEventsFromCache(MemoryTransaction transaction) {
        abandonedTransactionsCache.remove(transaction.getTransactionId());
    }

    @Override
    protected Iterator<LogMinerEvent> getTransactionEventIterator(MemoryTransaction transaction) {
        return transaction.getEvents().iterator();
    }

    @Override
    protected void finalizeTransactionCommit(String transactionId, Scn commitScn) {
        if (getConfig().isLobEnabled()) {
            // cache recently committed transactions by transaction id
            recentlyProcessedTransactionsCache.put(transactionId, commitScn);
        }
    }

    @Override
    protected void finalizeTransactionRollback(String transactionId, Scn rollbackScn) {
        transactionCache.remove(transactionId);
        abandonedTransactionsCache.remove(transactionId);
        if (getConfig().isLobEnabled()) {
            recentlyProcessedTransactionsCache.put(transactionId, rollbackScn);
        }
    }

    @Override
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        super.handleSchemaChange(row);
        if (row.getTableName() != null && getConfig().isLobEnabled()) {
            schemaChangesCache.add(row.getScn());
        }
    }

    @Override
    protected void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier) {
        if (abandonedTransactionsCache.contains(transactionId)) {
            LOGGER.warn("Event for abandoned transaction {}, skipped.", transactionId);
            return;
        }
        if (!isRecentlyProcessed(transactionId)) {
            MemoryTransaction transaction = getTransactionCache().get(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} not in cache for DML, creating.", transactionId);
                transaction = createTransaction(row);
                getTransactionCache().put(transactionId, transaction);
            }

            if (isTransactionOverEventThreshold(transaction)) {
                abandonTransactionOverEventThreshold(transaction);
                return;
            }

            int eventId = transaction.getNextEventId();
            if (transaction.getEvents().size() <= eventId) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at index {}", transactionId, eventId);
                transaction.getEvents().add(eventSupplier.get());
                metrics.calculateLagMetrics(row.getChangeTime());
            }

            metrics.setActiveTransactions(getTransactionCache().size());
        }
        else if (!getConfig().isLobEnabled()) {
            // Explicitly only log this warning when LobEnabled is false because its commonplace for a
            // transaction to be re-mined and therefore seen as already processed until the SCN low
            // watermark is advanced after a long transaction is committed.
            LOGGER.warn("Event for transaction {} has already been processed, skipped.", transactionId);
        }
    }

    @Override
    protected int getTransactionEventCount(MemoryTransaction transaction) {
        return transaction.getEvents().size();
    }

    @Override
    protected PreparedStatement createQueryStatement() throws SQLException {
        final String query = LogMinerQueryBuilder.build(getConfig());
        return jdbcConnection.connection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    protected Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            if (transactionCache.isEmpty() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    recentlyProcessedTransactionsCache.entrySet().removeIf(entry -> entry.getValue().compareTo(minStartScn) < 0);
                    schemaChangesCache.removeIf(scn -> scn.compareTo(minStartScn) < 0);
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return offsetContext.getScn();
        }
        else {
            if (!getLastProcessedScn().isNull() && getLastProcessedScn().compareTo(endScn) < 0) {
                // If the last processed SCN is before the endScn we need to use the last processed SCN as the
                // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
                endScn = getLastProcessedScn();
            }

            if (transactionCache.isEmpty()) {
                offsetContext.setScn(endScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return endScn;
        }
    }

    /**
     * Calculates the SCN as a watermark to abandon for long running transactions.
     * The criteria is do not let the offset SCN expire from archives older the specified retention hours.
     *
     * @param connection database connection, should not be {@code null}
     * @param retention duration to tolerate long running transactions before being abandoned, must not be {@code null}
     * @return an optional system change number as the watermark for transaction buffer abandonment
     */
    protected Optional<Scn> getLastScnToAbandon(OracleConnection connection, Duration retention) {
        try {
            if (getLastProcessedScn().isNull()) {
                return Optional.empty();
            }
            BigInteger scnToAbandon = connection.singleOptionalValue(
                    SqlUtils.getScnByTimeDeltaQuery(getLastProcessedScn(), retention),
                    rs -> rs.getBigDecimal(1).toBigInteger());
            return Optional.of(new Scn(scnToAbandon));
        }
        catch (SQLException e) {
            LOGGER.error(String.format("Cannot fetch SCN %s by given duration to calculate SCN to abandon", getLastProcessedScn()), e);
            metrics.incrementErrorCount();
            return Optional.empty();
        }
    }

    @Override
    protected void abandonTransactionOverEventThreshold(MemoryTransaction transaction) {
        super.abandonTransactionOverEventThreshold(transaction);
        abandonedTransactionsCache.add(transaction.getTransactionId());
    }

    @Override
    protected Scn getTransactionCacheMinimumScn() {
        return transactionCache.values().stream()
                .map(MemoryTransaction::getStartScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }
}
