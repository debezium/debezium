/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
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
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;

    /**
     * Cache of transactions, keyed based on the transaction's unique identifier
     */
    private final Map<String, MemoryTransaction> transactionCache = new HashMap<>();
    /**
     * Cache of processed transactions (committed or rolled back), keyed based on the transaction's unique identifier.
     */
    private final Map<String, Scn> recentlyProcessedTransactionsCache = new HashMap<>();
    private final Set<Scn> schemaChangesCache = new HashSet<>();

    public MemoryLogMinerEventProcessor(ChangeEventSourceContext context,
                                        OracleConnectorConfig connectorConfig,
                                        OracleConnection jdbcConnection,
                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                        OraclePartition partition,
                                        OracleOffsetContext offsetContext,
                                        OracleDatabaseSchema schema,
                                        LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics, jdbcConnection);
        this.dispatcher = dispatcher;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.metrics = (LogMinerStreamingChangeEventSourceMetrics) metrics;
    }

    @Override
    protected Map<String, MemoryTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    protected MemoryTransaction createTransaction(LogMinerEventRow row) {
        return new MemoryTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
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
    protected Iterator<LogMinerEvent> getTransactionEventIterator(MemoryTransaction transaction) {
        return transaction.getEvents().iterator();
    }

    @Override
    protected void cacheRecentlyCommittedTransaction(String transactionId, Scn commitScn) {
        getAbandonedTransactionsCache().remove(transactionId);
        if (getConfig().isLobEnabled()) {
            recentlyProcessedTransactionsCache.put(transactionId, commitScn);
            metrics.incrementRecentlyProcessedTransactions();
        }
    }

    @Override
    protected void cacheRecentlyRolledBackTransaction(String transactionId, Scn rollbackScn) {
        if (getConfig().isLobEnabled()) {
            recentlyProcessedTransactionsCache.put(transactionId, rollbackScn);
            metrics.incrementRecentlyProcessedTransactions();
        }
    }

    @Override
    protected String getFirstActiveTransactionKey() {
        final Iterator<String> keyIterator = transactionCache.keySet().iterator();
        return keyIterator.hasNext() ? keyIterator.next() : null;
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
        if (getAbandonedTransactionsCache().contains(transactionId)) {
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
                final LogMinerEvent event = eventSupplier.get();
                transaction.getEvents().add(event);
                if (event.isLobEvent()) {
                    transaction.setHasLobEvent();
                }
                metrics.calculateLagFromSource(row.getChangeTime());
            }

            metrics.setActiveTransactionCount(getTransactionCache().size());
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
        return jdbcConnection.connection().prepareStatement(getQueryString(),
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    protected Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final Scn minLobStartScn = getLobTransactionCacheMinimumScn();
            LOGGER.trace("Transaction cache minimum start SCN with LOB events: {}", minLobStartScn);
            if (minLobStartScn.isNull()) {
                // no LOB events, clearing cache and moving window
                recentlyProcessedTransactionsCache.clear();
                metrics.setRecentlyProcessedTransactions(0);
                return calculateScnNoLobEvents(endScn);
            }

            if (transactionCache.isEmpty() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                recentlyProcessedTransactionsCache.entrySet().removeIf(entry -> entry.getValue().compareTo(minLobStartScn) < 0);
                metrics.setRecentlyProcessedTransactions(recentlyProcessedTransactionsCache.size());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    schemaChangesCache.removeIf(scn -> scn.compareTo(minStartScn) < 0);
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }

            return offsetContext.getScn();
        }
        else {
            return calculateScnNoLobEvents(endScn);
        }
    }

    private Scn calculateScnNoLobEvents(Scn endScn) throws InterruptedException {
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

    @Override
    protected Scn getTransactionCacheMinimumScn() {
        return transactionCache.values().stream()
                .map(MemoryTransaction::getStartScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }

    @Override
    protected Scn getLobTransactionCacheMinimumScn() {
        return transactionCache.values().stream().filter(MemoryTransaction::hasLobEvent)
                .map(MemoryTransaction::getStartScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }

    @Override
    protected Optional<MemoryTransaction> getOldestTransactionInCache() {
        MemoryTransaction transaction = null;
        if (!transactionCache.isEmpty()) {
            // Seed with the first element
            transaction = transactionCache.values().iterator().next();
            for (MemoryTransaction entry : transactionCache.values()) {
                int comparison = entry.getStartScn().compareTo(transaction.getStartScn());
                if (comparison < 0) {
                    // if entry has a smaller scn, it came before.
                    transaction = entry;
                }
                else if (comparison == 0) {
                    // if entry has an equal scn, compare the change times.
                    if (entry.getChangeTime().isBefore(transaction.getChangeTime())) {
                        transaction = entry;
                    }
                }
            }
        }
        return Optional.ofNullable(transaction);
    }

}
