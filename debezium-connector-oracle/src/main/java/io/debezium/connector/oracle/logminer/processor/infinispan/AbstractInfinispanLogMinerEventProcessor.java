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
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.LogMinerQueryBuilder;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * An implementation of {@link io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor}
 * that uses Infinispan to persist the transaction cache across restarts on disk.
 *
 * @author Chris Cranford
 */
public abstract class AbstractInfinispanLogMinerEventProcessor extends AbstractLogMinerEventProcessor<InfinispanTransaction> implements CacheProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInfinispanLogMinerEventProcessor.class);

    private final OracleConnection jdbcConnection;
    private final OracleStreamingChangeEventSourceMetrics metrics;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final EventDispatcher<TableId> dispatcher;
    private final ChangeEventSourceContext context;

    private Scn currentOffsetScn = Scn.NULL;
    private Scn currentOffsetCommitScn = Scn.NULL;
    private Scn lastCommittedScn = Scn.NULL;
    private Scn maxCommittedScn = Scn.NULL;

    public AbstractInfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
                                                    OracleConnectorConfig connectorConfig,
                                                    OracleConnection jdbcConnection,
                                                    EventDispatcher<TableId> dispatcher,
                                                    OraclePartition partition,
                                                    OracleOffsetContext offsetContext,
                                                    OracleDatabaseSchema schema,
                                                    OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics);
        this.jdbcConnection = jdbcConnection;
        this.metrics = metrics;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.dispatcher = dispatcher;
        this.context = context;
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
        return new InfinispanTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        List<String> eventKeys = getEventCache().keySet()
                .stream()
                .filter(k -> k.startsWith(row.getTransactionId() + "-"))
                .collect(Collectors.toList());

        for (String eventKey : eventKeys) {
            final LogMinerEvent event = getEventCache().get(eventKey);
            if (event != null && event.getRowId().equals(row.getRowId())) {
                LOGGER.trace("Undo applied for event {}.", event);
                getEventCache().remove(eventKey);
            }
        }
    }

    @Override
    public Scn process(Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        counters.reset();

        try (PreparedStatement statement = createQueryStatement()) {
            LOGGER.debug("Fetching results for SCN [{}, {}]", startScn, endScn);
            statement.setFetchSize(getConfig().getMaxQueueSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, startScn.toString());
            statement.setString(2, endScn.toString());

            Instant queryStart = Instant.now();
            try (ResultSet resultSet = statement.executeQuery()) {
                metrics.setLastDurationOfBatchCapturing(Duration.between(queryStart, Instant.now()));

                Instant startProcessTime = Instant.now();
                processResults(resultSet);

                Duration totalTime = Duration.between(startProcessTime, Instant.now());
                metrics.setLastCapturedDmlCount(counters.dmlCount);

                if (counters.dmlCount > 0 || counters.commitCount > 0 || counters.rollbackCount > 0) {
                    warnPotentiallyStuckScn(currentOffsetScn, currentOffsetCommitScn);

                    currentOffsetScn = offsetContext.getScn();
                    if (offsetContext.getCommitScn() != null) {
                        currentOffsetCommitScn = offsetContext.getCommitScn();
                    }
                }

                LOGGER.debug("{}.", counters);
                LOGGER.debug("Processed in {} ms. Log: {}. Offset SCN: {}, Offset Commit SCN: {}, Active Transactions: {}, Sleep: {}",
                        totalTime.toMillis(), metrics.getLagFromSourceInMilliseconds(), offsetContext.getScn(),
                        offsetContext.getCommitScn(), metrics.getNumberOfActiveTransactions(),
                        metrics.getMillisecondToSleepBetweenMiningQuery());

                metrics.addProcessedRows(counters.rows);
                return calculateNewStartScn(endScn);
            }
        }
    }

    @Override
    protected void processRow(LogMinerEventRow row) throws SQLException, InterruptedException {
        final String transactionId = row.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.trace("Transaction {} has been seen by connector, skipped.", transactionId);
            return;
        }
        super.processRow(row);
    }

    @Override
    public void abandonTransactions(Duration retention) {
        // no-op, transactions are never abandoned
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return getSchemaChangesCache().containsKey(row.getScn().toString());
    }

    @Override
    protected void handleCommit(LogMinerEventRow row) throws InterruptedException {
        final String transactionId = row.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.debug("\tTransaction is already committed, skipped.");
            return;
        }

        final InfinispanTransaction transaction = getTransactionCache().get(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} not found.", transactionId);
            return;
        }
        else {
            // todo: Infinispan bug?
            // When interacting with ISPN with a remote server configuration, the expected
            // behavior was that calling the remove method on the cache would return the
            // existing entry and remove it from the cache; however it always returned null.
            //
            // For now, we're going to use get to obtain the value and then remove it after-the-fact.
            getTransactionCache().remove(transactionId);
        }

        final boolean skipExcludedUserName;
        if (transaction.getUserName() == null && transaction.getNumberOfEvents() > 0) {
            LOGGER.debug("Got transaction with null username {}", transaction);
            skipExcludedUserName = false;
        }
        else if (getConfig().getLogMiningUsernameExcludes().contains(transaction.getUserName())) {
            LOGGER.trace("Skipping transaction with excluded username {}", transaction);
            skipExcludedUserName = true;
        }
        else {
            skipExcludedUserName = false;
        }

        final Scn smallestScn = getTransactionCacheMinimumScn();
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);

        final Scn commitScn = row.getScn();
        final Scn offsetCommitScn = offsetContext.getCommitScn();
        if ((offsetCommitScn != null && offsetCommitScn.compareTo(commitScn) > 0) || lastCommittedScn.compareTo(commitScn) > 0) {
            LOGGER.debug("Transaction {} has already been processed. Commit SCN in offset is {} while commit SCN of transaction is {} and last seen committed SCN is {}.",
                    transactionId, offsetCommitScn, commitScn, lastCommittedScn);
            getTransactionCache().remove(transactionId);
            metrics.setActiveTransactions(getTransactionCache().size());
            removeEventsWithTransaction(transaction);
            return;
        }

        counters.commitCount++;
        Instant start = Instant.now();

        int numEvents = getTransactionEventCount(transaction);

        LOGGER.trace("Commit: (smallest SCN {}) {}", smallestScn, row);
        LOGGER.trace("Transaction {} has {} events", transactionId, numEvents);

        BlockingConsumer<LogMinerEvent> delegate = new BlockingConsumer<LogMinerEvent>() {
            private int numEvents = getTransactionEventCount(transaction);

            @Override
            public void accept(LogMinerEvent event) throws InterruptedException {
                // Update SCN in offset context only if processed SCN less than SCN of other transactions
                if (smallestScn.isNull() || commitScn.compareTo(smallestScn) < 0) {
                    offsetContext.setScn(event.getScn());
                    metrics.setOldestScn(event.getScn());
                }

                offsetContext.setTransactionId(transactionId);
                offsetContext.setSourceTime(event.getChangeTime());
                offsetContext.setTableId(event.getTableId());
                if (--numEvents == 0) {
                    // reached the last event update the commit scn in the offsets
                    offsetContext.setCommitScn(commitScn);
                }

                final DmlEvent dmlEvent = (DmlEvent) event;
                if (!skipExcludedUserName) {
                    dispatcher.dispatchDataChangeEvent(event.getTableId(),
                            new LogMinerChangeRecordEmitter(
                                    partition,
                                    offsetContext,
                                    dmlEvent.getEventType(),
                                    dmlEvent.getDmlEntry().getOldValues(),
                                    dmlEvent.getDmlEntry().getNewValues(),
                                    getSchema().tableFor(event.getTableId()),
                                    Clock.system()));
                }
            }
        };

        int eventCount = 0;
        try (TransactionCommitConsumer commitConsumer = new TransactionCommitConsumer(delegate, getConfig(), getSchema())) {
            for (int i = 0; i < transaction.getNumberOfEvents(); ++i) {
                if (!context.isRunning()) {
                    return;
                }

                final LogMinerEvent event = getEventCache().get(transaction.getEventId(i));
                if (event == null) {
                    // If an event is undone, it gets removed from the cache at undo time.
                    // This means that the call to get could return a null event and we
                    // should silently ignore it.
                    continue;
                }

                eventCount++;
                LOGGER.trace("Dispatching event {} {}", transaction.getEventId(i), event.getEventType());
                commitConsumer.accept(event);
            }
        }

        lastCommittedScn = Scn.valueOf(commitScn.longValue());
        if (transaction.getNumberOfEvents() > 0 && !skipExcludedUserName) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
        }
        else {
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }

        metrics.calculateLagMetrics(row.getChangeTime());
        if (lastCommittedScn.compareTo(maxCommittedScn) > 0) {
            maxCommittedScn = lastCommittedScn;
        }

        // cache recently committed transactions by transaction id
        getProcessedTransactionsCache().put(transactionId, commitScn.toString());

        // Clear the event queue for the transaction
        removeEventsWithTransaction(transaction);

        metrics.incrementCommittedTransactions();
        metrics.setActiveTransactions(getTransactionCache().size());
        metrics.incrementCommittedDmlCount(eventCount);
        metrics.setCommittedScn(commitScn);
        metrics.setOffsetScn(offsetContext.getScn());
        metrics.setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    @Override
    protected void handleRollback(LogMinerEventRow row) {
        final InfinispanTransaction transaction = getTransactionCache().get(row.getTransactionId());
        if (transaction != null) {
            removeEventsWithTransaction(transaction);
            getTransactionCache().remove(row.getTransactionId());
            getProcessedTransactionsCache().put(row.getTransactionId(), row.getScn().toString());

            metrics.setActiveTransactions(getTransactionCache().size());
            metrics.incrementRolledBackTransactions();
            metrics.addRolledBackTransactionId(row.getTransactionId());

            counters.rollbackCount++;
        }
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
            String eventKey = transaction.getEventId(transaction.getNextEventId());
            if (!getEventCache().containsKey(eventKey)) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at key {}", transactionId, eventKey);
                getEventCache().put(eventKey, eventSupplier.get());
                metrics.calculateLagMetrics(row.getChangeTime());
            }
            // When using Infinispan, this extra put is required so that the state is properly synchronized
            getTransactionCache().put(transactionId, transaction);
            metrics.setActiveTransactions(getTransactionCache().size());
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
                .parallelStream()
                .filter(k -> k.startsWith(transaction.getTransactionId() + "-"))
                .count();
    }

    private PreparedStatement createQueryStatement() throws SQLException {
        final String query = LogMinerQueryBuilder.build(getConfig(), getSchema());
        return jdbcConnection.connection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private Scn calculateNewStartScn(Scn endScn) throws InterruptedException {

        // Cleanup caches based on current state of the transaction cache
        final Scn minCacheScn = getTransactionCacheMinimumScn();
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
            metrics.setOldestScn(endScn);
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
}
