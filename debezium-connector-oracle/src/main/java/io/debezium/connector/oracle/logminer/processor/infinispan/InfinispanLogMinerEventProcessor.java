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
import io.debezium.connector.oracle.logminer.events.Transaction;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;
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
public class InfinispanLogMinerEventProcessor extends AbstractLogMinerEventProcessor {

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
        this.recentlyCommittedTransactionsCache = createCache(manager, connectorConfig, "committed-transactions");
        this.rollbackTransactionsCache = createCache(manager, connectorConfig, "rollback-transactions");
        this.schemaChangesCache = createCache(manager, connectorConfig, "schema-changes");
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
    protected TransactionCache<?> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public void close() throws Exception {
        if (getConfig().isLogMiningBufferDropOnStop()) {
            // Since the buffers are to be dropped on stop, clear them before stopping provider.
            transactionCache.clear();
            recentlyCommittedTransactionsCache.clear();
            rollbackTransactionsCache.clear();
            schemaChangesCache.clear();
        }

        recentlyCommittedTransactionsCache.stop();
        rollbackTransactionsCache.stop();
        schemaChangesCache.stop();
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
                metrics.setLastDurationOfBatchCapturing(totalTime);

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

        final Transaction transaction = transactionCache.remove(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} not found.", transactionId);
            return;
        }

        boolean skipExcludedUserName = false;
        if (transaction.getUserName() == null && !transaction.getEvents().isEmpty()) {
            LOGGER.debug("Got transaction with null username {}", transaction);
        }
        else if (getConfig().getLogMiningUsernameExcludes().contains(transaction.getUserName())) {
            LOGGER.trace("Skipping transaction with excluded username {}", transaction);
            skipExcludedUserName = true;
        }

        final Scn smallestScn = transactionCache.getMinimumScn();
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);

        final Scn commitScn = row.getScn();
        final Scn offsetCommitScn = offsetContext.getCommitScn();
        if ((offsetCommitScn != null && offsetCommitScn.compareTo(commitScn) > 0) || lastCommittedScn.compareTo(commitScn) > 0) {
            LOGGER.debug("Transaction {} has already been processed. Commit SCN in offset is {} while commit SCN of transaction is {} and last seen committed SCN is {}.",
                    transactionId, offsetCommitScn, commitScn, lastCommittedScn);
            metrics.setActiveTransactions(transactionCache.size());
            return;
        }

        counters.commitCount++;
        Instant start = Instant.now();
        getReconciliation().reconcile(transaction);

        int numEvents = transaction.getEvents().size();

        LOGGER.trace("Commit: (smallest SCN {}) {}", smallestScn, row);
        LOGGER.trace("Transaction {} has {} events", transactionId, numEvents);

        for (LogMinerEvent event : transaction.getEvents()) {
            if (!context.isRunning()) {
                return;
            }

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

            // after reconciliation all events should be DML
            // todo: do we want to move dml entry up and just let it be null to avoid cast?
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

        lastCommittedScn = Scn.valueOf(commitScn.longValue());
        if (!transaction.getEvents().isEmpty() && !skipExcludedUserName) {
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

        metrics.incrementCommittedTransactions();
        metrics.setActiveTransactions(transactionCache.size());
        metrics.incrementCommittedDmlCount(transaction.getEvents().size());
        metrics.setCommittedScn(commitScn);
        metrics.setOffsetScn(offsetContext.getScn());
        metrics.setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    @Override
    protected void handleRollback(LogMinerEventRow row) {
        final Transaction transaction = transactionCache.get(row.getTransactionId());
        if (transaction != null) {
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
            Transaction transaction = getTransactionCache().get(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} is not in cache, creating.", transactionId);
                transaction = new Transaction(transactionId, row.getScn(), row.getChangeTime(), row.getUserName());
            }
            int eventId = transaction.getNextEventId();
            if (transaction.getEvents().size() <= eventId) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at index {}", transactionId, eventId);
                transaction.getEvents().add(eventSupplier.get());
                metrics.calculateLagMetrics(row.getChangeTime());
            }
            // When using Infinispan, this extra put is required so that the state is properly synchronized
            getTransactionCache().put(transactionId, transaction);
            metrics.setActiveTransactions(getTransactionCache().size());
        }
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
}
