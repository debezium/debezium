/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
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
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.LogMinerQueryBuilder;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * A {@link LogMinerEventProcessor} that uses the JVM heap to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerEventProcessor extends AbstractLogMinerEventProcessor<MemoryTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryLogMinerEventProcessor.class);

    private final ChangeEventSourceContext context;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final OracleStreamingChangeEventSourceMetrics metrics;
    private final MemoryTransactionCache transactionCache;
    private final Map<String, Scn> recentlyCommittedTransactionsCache = new HashMap<>();
    private final Set<Scn> schemaChangesCache = new HashSet<>();
    private final Set<String> abandonedTransactionsCache = new HashSet<>();
    private final Set<String> rollbackTransactionsCache = new HashSet<>();

    private Scn currentOffsetScn = Scn.NULL;
    private Scn currentOffsetCommitScn = Scn.NULL;
    private Scn lastCommittedScn = Scn.NULL;
    private Scn maxCommittedScn = Scn.NULL;

    public MemoryLogMinerEventProcessor(ChangeEventSourceContext context,
                                        OracleConnectorConfig connectorConfig,
                                        OracleConnection jdbcConnection,
                                        EventDispatcher<TableId> dispatcher,
                                        OraclePartition partition,
                                        OracleOffsetContext offsetContext,
                                        OracleDatabaseSchema schema,
                                        OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics);
        this.context = context;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.metrics = metrics;
        this.transactionCache = new MemoryTransactionCache();
    }

    @Override
    protected TransactionCache<MemoryTransaction, ?> getTransactionCache() {
        return transactionCache;
    }

    @Override
    protected MemoryTransaction createTransaction(LogMinerEventRow row) {
        return new MemoryTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        final MemoryTransaction transaction = getTransactionCache().get(row.getTransactionId());
        if (transaction == null) {
            LOGGER.warn("Cannot undo change '{}' since transaction was not found.", row);
        }
        else {
            transaction.removeEventWithRowId(row.getRowId());
        }
    }

    @Override
    public void close() throws Exception {
        // close any resources used here
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
    public void abandonTransactions(Duration retention) {
        if (!Duration.ZERO.equals(retention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(jdbcConnection, offsetScn, retention);
            lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
                LOGGER.warn("All transactions with SCN <= {} will be abandoned.", thresholdScn);
                Scn smallestScn = transactionCache.getMinimumScn();
                if (!smallestScn.isNull()) {
                    if (thresholdScn.compareTo(smallestScn) < 0) {
                        thresholdScn = smallestScn;
                    }

                    Iterator<Map.Entry<String, MemoryTransaction>> iterator = transactionCache.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, MemoryTransaction> entry = iterator.next();
                        if (entry.getValue().getStartScn().compareTo(thresholdScn) <= 0) {
                            LOGGER.warn("Transaction {} is being abandoned.", entry.getKey());
                            abandonedTransactionsCache.add(entry.getKey());
                            iterator.remove();

                            metrics.addAbandonedTransactionId(entry.getKey());
                            metrics.setActiveTransactions(transactionCache.size());
                        }
                    }

                    // Update the oldest scn metric are transaction abandonment
                    smallestScn = transactionCache.getMinimumScn();
                    metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);
                }

                offsetContext.setScn(thresholdScn);
            });
        }
    }

    @Override
    protected boolean isRecentlyCommitted(String transactionId) {
        return recentlyCommittedTransactionsCache.containsKey(transactionId);
    }

    @Override
    protected boolean isTransactionIdAllowed(String transactionId) {
        if (abandonedTransactionsCache.contains(transactionId)) {
            LOGGER.warn("Event for abandoned transaction {}, skipped.", transactionId);
            return false;
        }
        if (rollbackTransactionsCache.contains(transactionId)) {
            LOGGER.warn("Event for rolled back transaction {}, skipped.", transactionId);
            return false;
        }
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
            LOGGER.trace("Event for already committed transaction {}, skipped.", transactionId);
            return false;
        }
        return true;
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return schemaChangesCache.contains(row.getScn());
    }

    @Override
    protected void handleCommit(LogMinerEventRow row) throws InterruptedException {
        final String transactionId = row.getTransactionId();
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
            return;
        }

        final MemoryTransaction transaction = transactionCache.remove(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} not found.", transactionId);
            return;
        }

        final Scn smallestScn = transactionCache.getMinimumScn();
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);
        abandonedTransactionsCache.remove(transactionId);

        final Scn commitScn = row.getScn();
        final Scn offsetCommitScn = offsetContext.getCommitScn();
        if ((offsetCommitScn != null && offsetCommitScn.compareTo(commitScn) > 0) || lastCommittedScn.compareTo(commitScn) > 0) {
            LOGGER.debug("Transaction {} has already been processed. Commit SCN in offset is {} while commit SCN " +
                    "of transaction is {} and last seen committed SCN is {}.",
                    transactionId, offsetCommitScn, commitScn, lastCommittedScn);
            metrics.setActiveTransactions(transactionCache.size());
            return;
        }

        counters.commitCount++;
        Instant start = Instant.now();

        int numEvents = transaction.getEvents().size();

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
            for (LogMinerEvent event : transaction.getEvents()) {
                if (!context.isRunning()) {
                    return;
                }

                eventCount++;
                LOGGER.trace("Dispatching event {} {}", eventCount, event.getEventType());
                commitConsumer.accept(event);
            }
        }

        lastCommittedScn = Scn.valueOf(commitScn.longValue());
        if (!transaction.getEvents().isEmpty()) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
        }
        else {
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }

        metrics.calculateLagMetrics(row.getChangeTime());
        if (lastCommittedScn.compareTo(maxCommittedScn) > 0) {
            maxCommittedScn = lastCommittedScn;
        }

        if (getConfig().isLobEnabled()) {
            // cache recently committed transactions by transaction id
            recentlyCommittedTransactionsCache.put(transactionId, commitScn);
        }

        metrics.incrementCommittedTransactions();
        metrics.setActiveTransactions(transactionCache.size());
        metrics.incrementCommittedDmlCount(eventCount);
        metrics.setCommittedScn(commitScn);
        metrics.setOffsetScn(offsetContext.getScn());
        metrics.setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    @Override
    protected void handleRollback(LogMinerEventRow row) {
        final MemoryTransaction transaction = transactionCache.get(row.getTransactionId());
        if (transaction != null) {
            transactionCache.remove(row.getTransactionId());
            abandonedTransactionsCache.remove(row.getTransactionId());
            rollbackTransactionsCache.add(row.getTransactionId());

            metrics.setActiveTransactions(transactionCache.size());
            metrics.incrementRolledBackTransactions();
            metrics.addRolledBackTransactionId(row.getTransactionId());

            counters.rollbackCount++;
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
        if (isTransactionIdAllowed(transactionId)) {
            MemoryTransaction transaction = getTransactionCache().get(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} not in cache for DML, creating.", transactionId);
                transaction = createTransaction(row);
                getTransactionCache().put(transactionId, transaction);
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
    }

    @Override
    protected int getTransactionEventCount(MemoryTransaction transaction) {
        return transaction.getEvents().size();
    }

    private PreparedStatement createQueryStatement() throws SQLException {
        final String query = LogMinerQueryBuilder.build(getConfig(), getSchema());
        return jdbcConnection.connection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private Scn calculateNewStartScn(Scn endScn) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            if (transactionCache.isEmpty() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                final Scn minStartScn = transactionCache.getMinimumScn();
                if (!minStartScn.isNull()) {
                    recentlyCommittedTransactionsCache.entrySet().removeIf(entry -> entry.getValue().compareTo(minStartScn) < 0);
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
                final Scn minStartScn = transactionCache.getMinimumScn();
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
     * @param offsetScn offset system change number, should not be {@code null}
     * @param retention duration to tolerate long running transactions before being abandoned, must not be {@code null}
     * @return an optional system change number as the watermark for transaction buffer abandonment
     */
    protected Optional<Scn> getLastScnToAbandon(OracleConnection connection, Scn offsetScn, Duration retention) {
        try {
            Float diffInDays = connection.singleOptionalValue(SqlUtils.diffInDaysQuery(offsetScn), rs -> rs.getFloat(1));
            if (diffInDays != null && (diffInDays * 24) > retention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference for transaction abandonment", e);
            metrics.incrementErrorCount();
            return Optional.of(offsetScn);
        }
    }

}
