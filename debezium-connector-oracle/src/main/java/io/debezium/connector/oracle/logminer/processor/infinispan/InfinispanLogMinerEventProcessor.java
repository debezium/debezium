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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
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
import io.debezium.connector.oracle.logminer.processor.Transaction;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;
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
public class InfinispanLogMinerEventProcessor extends AbstractLogMinerEventProcessor<InfinispanTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanLogMinerEventProcessor.class);

    private final OracleConnection jdbcConnection;
    private final OracleStreamingChangeEventSourceMetrics metrics;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final EventDispatcher<TableId> dispatcher;
    private final ChangeEventSourceContext context;

    /**
     * A cache that stores the complete {@link Transaction} object keyed by the unique transaction id.
     */
    private final InfinispanTransactionCache transactionCache;

    /**
     * A cache storing each of the raw events read from the LogMiner contents view.
     * This cache is keyed using the format of "<transaction-id>-<sequence>" where the sequence
     * is obtained from the {@link InfinispanTransaction#getEventId(int)} method.
     */
    private final Cache<String, LogMinerEvent> eventCache;

    /**
     * A cache storing recently committed transactions key by the unique transaction id and
     * the event's system change number.  This cache is used to filter events during re-mining
     * when LOB support is enabled to skip the processing of already emitted transactions.
     * Entries in this cache are removed when the offset low watermark (scn) advances beyond
     * the system change number associated with the transaction.
     */
    private final Cache<String, String> recentlyCommittedTransactionsCache;

    /**
     * A cache storing recently rolled back transactions keyed by the unique transaction id and
     * the event's system change number.  This cache is used to filter events during re-mining
     * when LOB support is enabled to skip the processing of already discarded transactions.
     * Entries in this cache are removed when the offset low watermark (scn) advances beyond
     * the system change number associated with the transaction.
     */
    private final Cache<String, String> rollbackTransactionsCache;

    /**
     * A cache storing recently emitted schema changes keyed by the system change number of the
     * schema change and the associated fully qualified TableId identifier value for the change.
     * This cache is used to filter events during re-mining when LOB support is enabled to skip
     * the processing of already emitted schema changes.  Entries in this cache are removed
     * when the offset low watermark (scn) advances beyond the system change number associated
     * with the schema change event.
     */
    private final Cache<String, String> schemaChangesCache;

    private Scn currentOffsetScn = Scn.NULL;
    private Scn currentOffsetCommitScn = Scn.NULL;
    private Scn lastCommittedScn = Scn.NULL;
    private Scn maxCommittedScn = Scn.NULL;

    public InfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
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

        final EmbeddedCacheManager manager = new DefaultCacheManager();
        this.transactionCache = new InfinispanTransactionCache(createCache(manager, connectorConfig, "transactions"));
        this.eventCache = createCache(manager, connectorConfig, "events");
        this.recentlyCommittedTransactionsCache = createCache(manager, connectorConfig, "committed-transactions");
        this.rollbackTransactionsCache = createCache(manager, connectorConfig, "rollback-transactions");
        this.schemaChangesCache = createCache(manager, connectorConfig, "schema-changes");

        displayCacheStatistics();
    }

    private void displayCacheStatistics() {
        LOGGER.info("Cache Statistics:");
        LOGGER.info("\tTransactions   : {}", transactionCache.size());
        LOGGER.info("\tCommitted Trxs : {}", recentlyCommittedTransactionsCache.size());
        LOGGER.info("\tRollback Trxs  : {}", rollbackTransactionsCache.size());
        LOGGER.info("\tSchema Changes : {}", schemaChangesCache.size());
        LOGGER.info("\tEvents         : {}", eventCache.size());
        if (!eventCache.isEmpty()) {
            for (String eventKey : eventCache.keySet()) {
                LOGGER.debug("\t\tFound Key: {}", eventKey);
            }
        }
    }

    private <K, V> Cache<K, V> createCache(EmbeddedCacheManager manager, OracleConnectorConfig connectorConfig, String name) {
        // todo: cache store configured similar to the database history configuration options
        final Configuration config = new ConfigurationBuilder()
                .persistence()
                .passivation(false)
                .addSingleFileStore()
                .segmented(false)
                .preload(true)
                .shared(false)
                .fetchPersistentState(true)
                .ignoreModifications(false)
                .location(connectorConfig.getLogMiningBufferLocation())
                .build();

        manager.defineConfiguration(name, config);
        return manager.getCache(name);
    }

    @Override
    protected TransactionCache<InfinispanTransaction, ?> getTransactionCache() {
        return transactionCache;
    }

    @Override
    protected InfinispanTransaction createTransaction(LogMinerEventRow row) {
        return new InfinispanTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        for (String eventKey : eventCache.keySet().stream().filter(k -> k.startsWith(row.getTransactionId() + "-")).collect(Collectors.toList())) {
            final LogMinerEvent event = eventCache.get(eventKey);
            if (event != null && event.getRowId().equals(row.getRowId())) {
                LOGGER.trace("Undo applied for event {}.", event);
                eventCache.remove(eventKey);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (getConfig().isLogMiningBufferDropOnStop()) {
            // Since the buffers are to be dropped on stop, clear them before stopping provider.
            eventCache.clear();
            transactionCache.clear();
            recentlyCommittedTransactionsCache.clear();
            rollbackTransactionsCache.clear();
            schemaChangesCache.clear();
        }

        recentlyCommittedTransactionsCache.stop();
        rollbackTransactionsCache.stop();
        schemaChangesCache.stop();
        eventCache.stop();
        transactionCache.close();
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
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
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
    protected boolean isTransactionIdAllowed(String transactionId) {
        if (rollbackTransactionsCache.containsKey(transactionId)) {
            LOGGER.warn("Event for transaction {} skipped as transaction is marked for rollback.", transactionId);
            return false;
        }
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
            LOGGER.warn("Event for transaction {} skipped as transaction was recently committed.", transactionId);
            return false;
        }
        return true;
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return schemaChangesCache.containsKey(row.getScn().toString());
    }

    @Override
    protected void handleCommit(LogMinerEventRow row) throws InterruptedException {
        final String transactionId = row.getTransactionId();
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
            return;
        }

        final InfinispanTransaction transaction = transactionCache.remove(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} not found.", transactionId);
            return;
        }

        final Scn smallestScn = transactionCache.getMinimumScn();
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);

        final Scn commitScn = row.getScn();
        final Scn offsetCommitScn = offsetContext.getCommitScn();
        if ((offsetCommitScn != null && offsetCommitScn.compareTo(commitScn) > 0) || lastCommittedScn.compareTo(commitScn) > 0) {
            LOGGER.debug("Transaction {} has already been processed. Commit SCN in offset is {} while commit SCN of transaction is {} and last seen committed SCN is {}.",
                    transactionId, offsetCommitScn, commitScn, lastCommittedScn);
            transactionCache.remove(transactionId);
            metrics.setActiveTransactions(transactionCache.size());
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
        };

        int eventCount = 0;
        try (TransactionCommitConsumer commitConsumer = new TransactionCommitConsumer(delegate, getConfig(), getSchema())) {
            for (int i = 0; i < transaction.getNumberOfEvents(); ++i) {
                if (!context.isRunning()) {
                    return;
                }

                final LogMinerEvent event = eventCache.get(transaction.getEventId(i));
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
        if (transaction.getNumberOfEvents() > 0) {
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
        recentlyCommittedTransactionsCache.put(transactionId, commitScn.toString());

        // Clear the event queue for the transaction
        removeEventsWithTransaction(transaction);

        metrics.incrementCommittedTransactions();
        metrics.setActiveTransactions(transactionCache.size());
        metrics.incrementCommittedDmlCount(eventCount);
        metrics.setCommittedScn(commitScn);
        metrics.setOffsetScn(offsetContext.getScn());
        metrics.setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    @Override
    protected void handleRollback(LogMinerEventRow row) {
        final InfinispanTransaction transaction = transactionCache.get(row.getTransactionId());
        if (transaction != null) {
            removeEventsWithTransaction(transaction);
            transactionCache.remove(row.getTransactionId());
            rollbackTransactionsCache.put(row.getTransactionId(), row.getScn().toString());

            metrics.setActiveTransactions(transactionCache.size());
            metrics.incrementRolledBackTransactions();
            metrics.addRolledBackTransactionId(row.getTransactionId());

            counters.rollbackCount++;
        }
    }

    @Override
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        super.handleSchemaChange(row);
        if (row.getTableName() != null) {
            schemaChangesCache.put(row.getScn().toString(), row.getTableId().identifier());
        }
    }

    @Override
    protected void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier) {
        if (isTransactionIdAllowed(transactionId)) {
            InfinispanTransaction transaction = getTransactionCache().get(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} is not in cache, creating.", transactionId);
                transaction = createTransaction(row);
            }
            String eventKey = transaction.getEventId(transaction.getNextEventId());
            if (!eventCache.containsKey(eventKey)) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at key {}", transactionId, eventKey);
                eventCache.put(eventKey, eventSupplier.get());
                metrics.calculateLagMetrics(row.getChangeTime());
            }
            // When using Infinispan, this extra put is required so that the state is properly synchronized
            getTransactionCache().put(transactionId, transaction);
            metrics.setActiveTransactions(getTransactionCache().size());
        }
    }

    @Override
    protected int getTransactionEventCount(InfinispanTransaction transaction) {
        // todo: implement indexed keys when ISPN supports them
        return (int) eventCache.keySet()
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
        final Scn minCacheScn = transactionCache.getMinimumScn();
        if (!minCacheScn.isNull()) {
            recentlyCommittedTransactionsCache.entrySet().removeIf(entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
            rollbackTransactionsCache.entrySet().removeIf(entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
            schemaChangesCache.entrySet().removeIf(entry -> Scn.valueOf(entry.getKey()).compareTo(minCacheScn) < 0);
        }
        else {
            recentlyCommittedTransactionsCache.clear();
            rollbackTransactionsCache.clear();
            schemaChangesCache.clear();
        }

        if (getConfig().isLobEnabled()) {
            if (transactionCache.isEmpty() && !maxCommittedScn.isNull()) {
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
            eventCache.remove(transaction.getEventId(i));
        }
    }
}
