/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.util.Loggings;

/**
 * An implementation of {@link io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor}
 * that uses Infinispan to persist the transaction cache across restarts on disk.
 *
 * @author Chris Cranford
 */
public abstract class AbstractInfinispanLogMinerEventProcessor extends AbstractLogMinerEventProcessor<InfinispanTransaction> implements CacheProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInfinispanLogMinerEventProcessor.class);

    private final OracleConnection jdbcConnection;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;

    public AbstractInfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
                                                    OracleConnectorConfig connectorConfig,
                                                    OracleConnection jdbcConnection,
                                                    EventDispatcher<OraclePartition, TableId> dispatcher,
                                                    OraclePartition partition,
                                                    OracleOffsetContext offsetContext,
                                                    OracleDatabaseSchema schema,
                                                    LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics);
        this.jdbcConnection = jdbcConnection;
        this.metrics = (LogMinerStreamingChangeEventSourceMetrics) metrics;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.dispatcher = dispatcher;
    }

    @Override
    public void displayCacheStatistics() {
        LOGGER.info("Overall Cache Statistics:");
        LOGGER.info("\tTransactions        : {}", getTransactionCache().size());
        LOGGER.info("\tRecent Transactions : {}", getProcessedTransactionsCache().size());
        LOGGER.info("\tSchema Changes      : {}", getSchemaChangesCache().size());
        LOGGER.info("\tEvents              : {}", getEventCache().size());
        if (!getEventCache().isEmpty()) {
            for (String eventKey : getEventCache().keySet()) {
                LOGGER.debug("\t\tFound Key: {}", eventKey);
            }
        }
    }

    @Override
    protected boolean isRecentlyProcessed(String transactionId) {
        return getProcessedTransactionsCache().containsKey(transactionId);
    }

    @Override
    protected InfinispanTransaction createTransaction(LogMinerEventRow row) {
        return new InfinispanTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        List<String> eventKeys = getTransactionKeysWithPrefix(row.getTransactionId() + "-");
        if (eventKeys.isEmpty() && isTransactionIdWithNoSequence(row.getTransactionId())) {
            // This means that Oracle LogMiner found an event that should be undone but its corresponding
            // undo entry was read in a prior mining session and the transaction's sequence could not be
            // resolved. In this case, lets locate the transaction based solely on XIDUSN and XIDSLT.
            final String transactionPrefix = getTransactionIdPrefix(row.getTransactionId());
            LOGGER.debug("Undo change refers to a transaction that has no explicit sequence, '{}'", row.getTransactionId());
            LOGGER.debug("Checking all transactions with prefix '{}'", transactionPrefix);
            eventKeys = getTransactionKeysWithPrefix(transactionPrefix);
            if (!eventKeys.isEmpty()) {
                // Enforce that the keys are always reverse sorted.
                eventKeys.sort(EventKeySortComparator.INSTANCE.reversed());

                for (String eventKey : eventKeys) {
                    final LogMinerEvent event = getEventCache().get(eventKey);
                    if (event != null && event.getRowId().equals(row.getRowId())) {
                        Loggings.logDebugAndTraceRecord(LOGGER, row, "Undo change on table '{}' applied to transaction '{}'", row.getTableId(), eventKey);
                        getEventCache().remove(eventKey);
                        return;
                    }
                }
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since event with row-id {} was not found.", row.getTableId(),
                        row.getRowId());
            }
            else if (!getConfig().isLobEnabled()) {
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since transaction '{}' was not found.", row.getTableId(),
                        row.getTransactionId());
            }
        }
        else {
            // Enforce that the keys are always reverse sorted.
            eventKeys.sort(EventKeySortComparator.INSTANCE.reversed());

            for (String eventKey : eventKeys) {
                final LogMinerEvent event = getEventCache().get(eventKey);
                if (event != null && event.getRowId().equals(row.getRowId())) {
                    LOGGER.debug("Undo applied for event {}.", event);
                    getEventCache().remove(eventKey);
                    return;
                }
            }
            Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since event with row-id {} was not found.", row.getTableId(),
                    row.getRowId());
        }
    }

    private List<String> getTransactionKeysWithPrefix(String prefix) {
        return getEventCache().keySet().stream().filter(k -> k.startsWith(prefix)).collect(Collectors.toList());
    }

    @Override
    protected void processRow(OraclePartition partition, LogMinerEventRow row) throws SQLException, InterruptedException {
        final String transactionId = row.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.debug("Transaction {} has been seen by connector, skipped.", transactionId);
            return;
        }
        super.processRow(partition, row);
    }

    @Override
    public void abandonTransactions(Duration retention) throws InterruptedException {
        // no-op, transactions are never abandoned
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return getSchemaChangesCache().containsKey(row.getScn().toString());
    }

    @Override
    protected InfinispanTransaction getAndRemoveTransactionFromCache(String transactionId) {
        // todo: Infinispan bug?
        // When interacting with ISPN with a remote server configuration, the expected
        // behavior was that calling the remove method on the cache would return the
        // existing entry and remove it from the cache; however it always returned null.
        //
        // For now, we're going to use get to obtain the value and then remove it after-the-fact.
        final InfinispanTransaction transaction = getTransactionCache().get(transactionId);
        if (transaction != null) {
            getTransactionCache().remove(transactionId);
        }
        return transaction;
    }

    @Override
    protected void removeTransactionAndEventsFromCache(InfinispanTransaction transaction) {
        removeEventsWithTransaction(transaction);
        getTransactionCache().remove(transaction.getTransactionId());
    }

    @Override
    protected Iterator<LogMinerEvent> getTransactionEventIterator(InfinispanTransaction transaction) {
        return new Iterator<LogMinerEvent>() {
            private final int count = transaction.getNumberOfEvents();

            private LogMinerEvent nextEvent;
            private int index = 0;

            @Override
            public boolean hasNext() {
                while (index < count) {
                    nextEvent = getEventCache().get(transaction.getEventId(index));
                    if (nextEvent == null) {
                        LOGGER.debug("Event {} must have been undone, skipped.", index);
                        // There are situations where an event will be removed from the cache when it is
                        // undone by the undo-row flag. The event id isn't re-used in this use case so
                        // the iterator automatically detects null entries and skips them by advancing
                        // to the next entry until either we've reached the number of events or detected
                        // a non-null entry available for return
                        index++;
                        continue;
                    }
                    break;
                }
                return index < count;
            }

            @Override
            public LogMinerEvent next() {
                index++;
                return nextEvent;
            }
        };
    }

    @Override
    protected void finalizeTransactionCommit(String transactionId, Scn commitScn) {
        // cache recently committed transactions by transaction id
        getProcessedTransactionsCache().put(transactionId, commitScn.toString());
    }

    @Override
    protected void finalizeTransactionRollback(String transactionId, Scn rollbackScn) {
        final InfinispanTransaction transaction = getTransactionCache().get(transactionId);
        if (transaction != null) {
            removeEventsWithTransaction(transaction);
            getTransactionCache().remove(transactionId);
        }
        getProcessedTransactionsCache().put(transactionId, rollbackScn.toString());
    }

    @Override
    protected void resetTransactionToStart(InfinispanTransaction transaction) {
        super.resetTransactionToStart(transaction);
        // Flush the change created by the super class to the transaction cache
        getTransactionCache().put(transaction.getTransactionId(), transaction);
    }

    @Override
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        super.handleSchemaChange(row);
        if (row.getTableName() != null) {
            getSchemaChangesCache().put(row.getScn().toString(), row.getTableId().identifier());
        }
    }

    @Override
    protected void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier) {
        if (!isRecentlyProcessed(transactionId)) {
            InfinispanTransaction transaction = getTransactionCache().get(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} is not in cache, creating.", transactionId);
                transaction = createTransaction(row);
            }

            if (isTransactionOverEventThreshold(transaction)) {
                abandonTransactionOverEventThreshold(transaction);
                return;
            }

            String eventKey = transaction.getEventId(transaction.getNextEventId());
            if (!getEventCache().containsKey(eventKey)) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at key {}", transactionId, eventKey);
                getEventCache().put(eventKey, eventSupplier.get());
                metrics.calculateLagFromSource(row.getChangeTime());
            }
            // When using Infinispan, this extra put is required so that the state is properly synchronized
            getTransactionCache().put(transactionId, transaction);
            metrics.setActiveTransactionCount(getTransactionCache().size());
        }
        else {
            LOGGER.warn("Event for transaction {} skipped as transaction has been processed.", transactionId);
        }
    }

    @Override
    protected int getTransactionEventCount(InfinispanTransaction transaction) {
        // todo: implement indexed keys when ISPN supports them
        return (int) getEventCache()
                .keySet()
                .stream()
                .filter(k -> k.startsWith(transaction.getTransactionId() + "-"))
                .count();
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

        // Cleanup caches based on current state of the transaction cache
        final Optional<InfinispanTransaction> oldestTransaction = getOldestTransactionInCache();
        final Scn minCacheScn;
        final Instant minCacheScnChangeTime;
        if (oldestTransaction.isPresent()) {
            minCacheScn = oldestTransaction.get().getStartScn();
            minCacheScnChangeTime = oldestTransaction.get().getChangeTime();
        }
        else {
            minCacheScn = Scn.NULL;
            minCacheScnChangeTime = null;
        }

        if (!minCacheScn.isNull()) {
            getProcessedTransactionsCache().entrySet().removeIf(entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
            getSchemaChangesCache().entrySet().removeIf(entry -> Scn.valueOf(entry.getKey()).compareTo(minCacheScn) < 0);
        }
        else {
            getProcessedTransactionsCache().clear();
            getSchemaChangesCache().clear();
        }

        if (getConfig().isLobEnabled()) {
            if (getTransactionCache().isEmpty() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                if (!minCacheScn.isNull()) {
                    offsetContext.setScn(minCacheScn.subtract(Scn.valueOf(1)));
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

            // update offsets
            offsetContext.setScn(endScn);
            metrics.setOldestScnDetails(minCacheScn, minCacheScnChangeTime);
            metrics.setOffsetScn(endScn);

            // optionally dispatch a heartbeat event
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);

            return endScn;
        }
    }

    private void removeEventsWithTransaction(InfinispanTransaction transaction) {
        // Clear the event queue for the transaction
        for (int i = 0; i < transaction.getNumberOfEvents(); ++i) {
            getEventCache().remove(transaction.getEventId(i));
        }
    }

    /**
     * A comparator that guarantees that the sort order applied to event keys is such that
     * they are treated as numerical values, sorted as numeric values rather than strings
     * which would allow "100" to come before "9".
     */
    private static class EventKeySortComparator implements Comparator<String> {

        public static EventKeySortComparator INSTANCE = new EventKeySortComparator();

        @Override
        public int compare(String o1, String o2) {
            if (o1 == null || !o1.contains("-")) {
                throw new IllegalStateException("Event Key must be in the format of <transaction>-<event>");
            }
            if (o2 == null || !o2.contains("-")) {
                throw new IllegalStateException("Event Key must be in the format of <transaction>-<event>");
            }
            final String[] s1 = o1.split("-");
            final String[] s2 = o2.split("-");

            // Compare transaction ids, these should generally be identical.
            int result = s1[0].compareTo(s2[0]);
            if (result == 0) {
                result = Long.compare(Long.parseLong(s1[1]), Long.parseLong(s2[1]));
            }
            return result;
        }
    }
}
