/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransactionFactory;
import io.debezium.connector.oracle.logminer.buffered.infinispan.EmbeddedInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransactionFactory;
import io.debezium.connector.oracle.logminer.buffered.infinispan.RemoteInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransactionFactory;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.RacCommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.ReadOnlyLogWriterFlushStrategy;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Loggings;
import io.debezium.util.Stopwatch;
import io.debezium.util.Strings;

/**
 * An implementation of {@link AbstractLogMinerStreamingChangeEventSource} to read changes from LogMiner
 * using uncommitted mode paired with heap and off-heap caches for in-flight transaction storage.
 * <p>
 * The event handler loop is executed in a separate executor.
 */
public class BufferedLogMinerStreamingChangeEventSource extends AbstractLogMinerStreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedLogMinerStreamingChangeEventSource.class);
    private static final Logger ABANDONED_DETAILS_LOGGER = LoggerFactory.getLogger(BufferedLogMinerStreamingChangeEventSource.class.getName() + ".AbandonedDetails");
    private static final String NO_SEQUENCE_TRX_ID_SUFFIX = "ffffffff";

    private final String queryString;
    private final CacheProvider<Transaction> cacheProvider;
    private final TransactionFactory<Transaction> transactionFactory;

    private Instant lastProcessedScnChangeTime = null;
    private Scn lastProcessedScn = Scn.NULL;

    public BufferedLogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                                      OracleConnection jdbcConnection,
                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                      ErrorHandler errorHandler,
                                                      Clock clock,
                                                      OracleDatabaseSchema schema,
                                                      Configuration jdbcConfig,
                                                      LogMinerStreamingChangeEventSourceMetrics streamingMetrics) {
        super(connectorConfig, jdbcConnection, dispatcher, errorHandler, clock, schema, jdbcConfig, streamingMetrics);

        this.queryString = new BufferedLogMinerQueryBuilder(connectorConfig).getQuery();
        this.cacheProvider = createCacheProvider(connectorConfig);
        this.transactionFactory = createTransactionFactory(connectorConfig);
    }

    @Override
    protected void executeLogMiningStreaming() throws Exception {

        try (LogWriterFlushStrategy flushStrategy = resolveFlushStrategy()) {

            Scn startScn = getOffsetContext().getScn();
            Scn endScn = Scn.NULL;

            Stopwatch watch = Stopwatch.accumulating().start();
            int miningStartAttempts = 1;

            prepareLogsForMining(false, startScn);

            while (getContext().isRunning()) {

                // Check if we should break when using archive log only mode
                if (getConfig().isArchiveLogOnlyMode()) {
                    if (waitForRangeAvailabilityInArchiveLogs(startScn, endScn)) {
                        break;
                    }
                }

                final Instant batchStartTime = Instant.now();

                updateDatabaseTimeDifference();

                Scn currentScn = getCurrentScn();
                getMetrics().setCurrentScn(currentScn);

                endScn = calculateUpperBounds(startScn, endScn, currentScn);
                if (endScn.isNull()) {
                    LOGGER.debug("Requested delay of mining by one iteration");
                    pauseBetweenMiningSessions();
                    continue;
                }

                flushStrategy.flush(getCurrentScn());

                if (isMiningSessionRestartRequired(watch) || checkLogSwitchOccurredAndUpdate()) {
                    // Mining session is active, so end the current session and restart if necessary
                    endMiningSession();
                    if (getConfig().isLogMiningRestartConnection()) {
                        prepareJdbcConnection(true);
                    }

                    prepareLogsForMining(true, startScn);

                    // Recreate the stop watch
                    watch = Stopwatch.accumulating().start();
                }

                if (startMiningSession(startScn, endScn, miningStartAttempts)) {
                    miningStartAttempts = 1;
                    startScn = process(startScn, endScn);

                    getMetrics().setLastBatchProcessingDuration(Duration.between(batchStartTime, Instant.now()));
                }
                else {
                    miningStartAttempts++;
                }

                captureJdbcSessionMemoryStatistics();

                pauseBetweenMiningSessions();

                if (getContext().isPaused()) {
                    executeBlockingSnapshot();
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            cacheProvider.close();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to gracefully shutdown the cache provider", e);
        }
    }

    /**
     * Resolves the Oracle LGWR buffer flushing strategy.
     *
     * @return the strategy to be used to flush Oracle's LGWR process, never {@code null}.
     */
    private LogWriterFlushStrategy resolveFlushStrategy() {
        if (getConfig().isLogMiningReadOnly()) {
            return new ReadOnlyLogWriterFlushStrategy();
        }
        if (getConfig().isRacSystem()) {
            return new RacCommitLogWriterFlushStrategy(getConfig(), getJdbcConfiguration(), getMetrics());
        }
        return new CommitLogWriterFlushStrategy(getConfig(), getConnection());
    }

    @SuppressWarnings("unchecked")
    private <T extends Transaction> CacheProvider<T> createCacheProvider(OracleConnectorConfig connectorConfig) {
        return (CacheProvider<T>) switch (connectorConfig.getLogMiningBufferType()) {
            case MEMORY -> new MemoryCacheProvider(connectorConfig);
            case INFINISPAN_EMBEDDED -> new EmbeddedInfinispanCacheProvider(connectorConfig);
            case INFINISPAN_REMOTE -> new RemoteInfinispanCacheProvider(connectorConfig);
            case EHCACHE -> new EhcacheCacheProvider(connectorConfig);
        };
    }

    @SuppressWarnings("unchecked")
    private <T extends Transaction> TransactionFactory<T> createTransactionFactory(OracleConnectorConfig connectorConfig) {
        return (TransactionFactory<T>) switch (connectorConfig.getLogMiningBufferType()) {
            case MEMORY -> new MemoryTransactionFactory();
            case INFINISPAN_EMBEDDED, INFINISPAN_REMOTE -> new InfinispanTransactionFactory();
            case EHCACHE -> new EhcacheTransactionFactory();
        };
    }

    @VisibleForTesting
    protected Scn process(Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        getBatchMetrics().reset();

        try (PreparedStatement statement = createQueryStatement()) {
            LOGGER.debug("Fetching results for SCN [{}, {}]", startScn, endScn);
            statement.setFetchSize(getConfig().getQueryFetchSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, startScn.toString());
            statement.setString(2, endScn.toString());

            if (getConfig().isLogMiningUseCteQuery()) {
                statement.setString(3, startScn.toString());
                statement.setString(4, endScn.toString());
            }

            executeAndProcessQuery(statement);

            logActiveTransactions();

            return calculateNewStartScn(startScn, endScn, getOffsetContext().getCommitScn().getMaxCommittedScn());
        }
    }

    @VisibleForTesting
    protected LogMinerTransactionCache<Transaction> getTransactionCache() {
        return cacheProvider.getTransactionCache();
    }

    @VisibleForTesting
    protected LogMinerCache<String, String> getProcessedTransactionsCache() {
        return cacheProvider.getProcessedTransactionsCache();
    }

    @VisibleForTesting
    protected LogMinerCache<String, String> getSchemaChangesCache() {
        return cacheProvider.getSchemaChangesCache();
    }

    private boolean isRecentlyProcessed(String transactionId) {
        return getProcessedTransactionsCache().containsKey(transactionId);
    }

    private boolean hasSchemaChangeBeenSeen(LogMinerEventRow event) {
        return getSchemaChangesCache().containsKey(event.getScn().toString());
    }

    private int getTransactionEventCount(Transaction transaction) {
        return getTransactionCache().getTransactionEventCount(transaction);
    }

    @VisibleForTesting
    protected PreparedStatement createQueryStatement() throws SQLException {
        final PreparedStatement statement = getConnection().connection()
                .prepareStatement(queryString,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
        statement.setQueryTimeout((int) getConnection().config().getQueryTimeout().toSeconds());
        return statement;
    }

    @Override
    protected void preProcessEvent(LogMinerEventRow event) {
        super.preProcessEvent(event);

        if (!EventType.MISSING_SCN.equals(event.getEventType())) {
            lastProcessedScn = event.getScn();
            lastProcessedScnChangeTime = event.getChangeTime();
        }
    }

    @Override
    protected boolean isEventSkipped(LogMinerEventRow event) {
        // Check whether the row has a table reference and if so, is the reference included by the filter.
        // If the reference isn't included, the row will be skipped entirely.
        if (event.getTableId() != null) {
            if (LogWriterFlushStrategy.isFlushTable(event.getTableId(), getConfig().getJdbcConfig().getUser(), getConfig().getLogMiningFlushTableName())) {
                LOGGER.trace("Skipped change associated with flush table '{}'", event.getTableId());
                return true;
            }

            // DDL events get filtered inside the DDL handler
            // We do the non-DDL ones here to cover multiple switch handlers in one place.
            if (isNonSchemaChangeEventSkipped(event)) {
                return true;
            }
        }

        final Transaction transaction = getTransactionCache().getTransaction(event.getTransactionId());
        if (transaction != null && isTransactionOverEventThreshold(transaction)) {
            abandonTransactionOverEventThreshold(transaction);
            return true;
        }

        return false;
    }

    @Override
    protected void handleStartEvent(LogMinerEventRow event) {
        final String transactionId = event.getTransactionId();
        if (!isRecentlyProcessed(transactionId)) {
            final Transaction transaction = getTransactionCache().getTransaction(transactionId);
            if (transaction == null) {
                getTransactionCache().addTransaction(transactionFactory.createTransaction(event));
                getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
            }
            else {
                LOGGER.trace("Transaction {} is not yet committed and START event detected.", transactionId);
                getTransactionCache().resetTransactionToStart(transaction);
            }
        }
    }

    @Override
    protected void handleCommitEvent(LogMinerEventRow row) throws InterruptedException {
        final String transactionId = row.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.debug("\tTransaction is already committed, skipped.");
            return;
        }

        final Transaction transaction = getTransactionCache().getAndRemoveTransaction(transactionId);
        if (transaction == null) {
            if (!getOffsetContext().getCommitScn().hasEventScnBeenHandled(row)) {
                LOGGER.debug("Transaction {} not found in cache with SCN {}, no events to commit.", transactionId, row.getScn());
            }

            // In the event the transaction was prematurely removed due to retention policy, when we do find
            // the transaction's commit in the logs in the future, we should remove the entry if it exists
            // to avoid any potential memory-leak with the cache.
            getTransactionCache().removeAbandonedTransaction(row.getTransactionId());
        }

        final Scn smallestScn = calculateSmallestScn();
        final Scn commitScn = row.getScn();
        if (getOffsetContext().getCommitScn().hasEventScnBeenHandled(row)) {
            if (transaction != null) {
                if (transaction.getNumberOfEvents() > 0) {
                    final Scn lastCommittedScn = getOffsetContext().getCommitScn().getCommitScnForRedoThread(row.getThread());
                    LOGGER.debug("Transaction {} has already been processed. "
                            + "Offset Commit SCN {}, Transaction Commit SCN {}, Last Seen Commit SCN {}.",
                            transactionId, getOffsetContext().getCommitScn(), commitScn, lastCommittedScn);
                }
                cleanupAfterTransactionRemovedFromCache(transaction, false);
                getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
            }
            return;
        }

        int numEvents = (transaction == null) ? 0 : getTransactionEventCount(transaction);

        // There are situations where Oracle records empty transactions in the redo and these
        // do not make any changes. In such cases, LogMiner fails to reconcile the thread id
        // for the commit, and leaves it assigned to 0. This ultimately leads to the commit
        // recorded for the wrong redo thread. Given that we cannot just "guess" the thread
        // as there may not have been a transaction recorded at all when LOB isn't enabled,
        // all we can do is ignore the COMMIT.
        final boolean skipCommit = row.getThread() == 0 && numEvents == 0;

        LOGGER.debug("{} transaction {} with {} events (scn: {}, thread: {}, oldest buffer scn: {}): {}",
                skipCommit ? "Skipping commit for" : "Committing",
                transactionId, numEvents, row.getScn(), row.getThread(), smallestScn, row);

        if (skipCommit) {
            if (transaction != null) {
                cleanupAfterTransactionRemovedFromCache(transaction, false);
            }
            return;
        }

        getBatchMetrics().commitObserved();

        // When a COMMIT is received, regardless of the number of events it has, it still
        // must be recorded in the commit scn for the node to guarantee updates to the
        // offsets. This must be done prior to dispatching the transaction-commit or the
        // heartbeat event that follows commit dispatch.
        getOffsetContext().getCommitScn().recordCommit(row);

        Instant start = Instant.now();
        boolean dispatchTransactionCommittedEvent = false;
        if (numEvents > 0) {
            final boolean skipEvents = isTransactionSkippedAtCommit(transaction);
            dispatchTransactionCommittedEvent = !skipEvents;
            final ZoneOffset databaseOffset = getMetrics().getDatabaseOffset();
            TransactionCommitConsumer.Handler<LogMinerEvent> delegate = (event, eventsProcessed) -> {
                // Update SCN in offset context only if processed SCN less than SCN of other transactions
                if (smallestScn.isNull() || commitScn.compareTo(smallestScn) < 0) {
                    getOffsetContext().setScn(event.getScn());
                    getMetrics().setOldestScnDetails(event.getScn(), event.getChangeTime());
                }

                getOffsetContext().setEventScn(event.getScn());
                getOffsetContext().setEventCommitScn(row.getScn());
                getOffsetContext().setTransactionId(transactionId);
                getOffsetContext().setUserName(transaction.getUserName());
                getOffsetContext().setSourceTime(event.getChangeTime().minusSeconds(databaseOffset.getTotalSeconds()));
                getOffsetContext().setTableId(event.getTableId());
                getOffsetContext().setRedoThread(row.getThread());
                getOffsetContext().setRsId(event.getRsId());
                getOffsetContext().setRowId(event.getRowId());
                getOffsetContext().setCommitTime(row.getChangeTime().minusSeconds(databaseOffset.getTotalSeconds()));

                if (eventsProcessed == 1) {
                    getOffsetContext().setStartScn(event.getScn());
                    getOffsetContext().setStartTime(event.getChangeTime().minusSeconds(databaseOffset.getTotalSeconds()));
                }

                if (event instanceof RedoSqlDmlEvent) {
                    getOffsetContext().setRedoSql(((RedoSqlDmlEvent) event).getRedoSql());
                }

                final DmlEvent dmlEvent = (DmlEvent) event;
                if (!skipEvents) {
                    LogMinerChangeRecordEmitter logMinerChangeRecordEmitter;
                    if (dmlEvent instanceof TruncateEvent) {
                        // a truncate event is seen by logminer as a DDL event type.
                        // So force this here to be a Truncate Operation.
                        logMinerChangeRecordEmitter = new LogMinerChangeRecordEmitter(
                                getConfig(),
                                getPartition(),
                                getOffsetContext(),
                                Envelope.Operation.TRUNCATE,
                                dmlEvent.getDmlEntry().getOldValues(),
                                dmlEvent.getDmlEntry().getNewValues(),
                                getSchema().tableFor(event.getTableId()),
                                getSchema(),
                                Clock.system());
                    }
                    else {
                        logMinerChangeRecordEmitter = new LogMinerChangeRecordEmitter(
                                getConfig(),
                                getPartition(),
                                getOffsetContext(),
                                dmlEvent.getEventType(),
                                dmlEvent.getDmlEntry().getOldValues(),
                                dmlEvent.getDmlEntry().getNewValues(),
                                getSchema().tableFor(event.getTableId()),
                                getSchema(),
                                Clock.system());
                    }
                    getEventDispatcher().dispatchDataChangeEvent(getPartition(), event.getTableId(), logMinerChangeRecordEmitter);
                }

                // Clear redo SQL
                getOffsetContext().setRedoSql(null);
            };
            try (TransactionCommitConsumer commitConsumer = new TransactionCommitConsumer(delegate, getConfig(), getSchema())) {
                getTransactionCache().forEachEvent(transaction, event -> {
                    if (!getContext().isRunning()) {
                        return false;
                    }
                    LOGGER.trace("Dispatching event {}", event.getEventType());
                    commitConsumer.accept(event);
                    return true;
                });
            }
        }

        getOffsetContext().setEventScn(commitScn);
        getOffsetContext().setRsId(row.getRsId());
        getOffsetContext().setRowId("");
        getOffsetContext().setStartScn(Scn.NULL);
        getOffsetContext().setCommitTime(null);
        getOffsetContext().setStartTime(null);

        if (dispatchTransactionCommittedEvent) {
            getEventDispatcher().dispatchTransactionCommittedEvent(getPartition(), getOffsetContext(), transaction.getChangeTime());
        }
        else {
            getEventDispatcher().dispatchHeartbeatEvent(getPartition(), getOffsetContext());
        }

        if (transaction != null) {
            finalizeTransaction(transactionId, commitScn, false);
            cleanupAfterTransactionRemovedFromCache(transaction, false);
            getMetrics().calculateLagFromSource(row.getChangeTime());
            getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
        }

        updateCommitMetrics(row, Duration.between(start, Instant.now()));
    }

    @Override
    protected void handleRollbackEvent(LogMinerEventRow event) {
        final String transactionId = event.getTransactionId();
        if (getTransactionCache().containsTransaction(transactionId)) {
            LOGGER.debug("Transaction {} was rolled back.", transactionId);
            finalizeTransaction(transactionId, event.getScn(), true);
            getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
        }
        else {
            LOGGER.debug("Transaction {} not found in cache, no events to rollback.", transactionId);
            // In the event the transaction was prematurely removed due to retention policy, when we do find
            // the transaction's rollback in the logs in the future, we should remove the entry if it exists
            // to avoid any potential memory-leak with the cache.
            getTransactionCache().removeAbandonedTransaction(transactionId);
        }

        getMetrics().incrementRolledBackTransactionCount();
        getMetrics().addRolledBackTransactionId(transactionId);
        getBatchMetrics().rollbackObserved();
    }

    @Override
    protected void handleSchemaChangeEvent(LogMinerEventRow event) throws InterruptedException {
        if (isSchemaChangeEventSkipped(event)) {
            return;
        }

        if (hasSchemaChangeBeenSeen(event)) {
            LOGGER.trace("DDL: Scn {}, SQL '{}' has already been processed, skipped.", event.getScn(), event.getRedoSql());
            return;
        }

        if (!Strings.isNullOrEmpty(event.getTableName())) {
            Loggings.logDebugAndTraceRecord(LOGGER, event, "Processing DDL event with SCN {}: {}",
                    event.getScn(), event.getRedoSql());

            if (canAdvanceLowerScnBoundaryOnSchemaChange(event)) {
                LOGGER.debug("Schema change advanced offset SCN to {}", event.getScn());
                getOffsetContext().setScn(event.getScn());
            }

            // Should always advance the commit SCN point with schema changes
            LOGGER.debug("Schema change advanced offset commit SCN to {} for thread {}", event.getScn(), event.getThread());
            getOffsetContext().getCommitScn().recordCommit(event);

            if (getConfig().isLobEnabled()) {
                getSchemaChangesCache().put(event.getScn().toString(), event.getTableId().identifier());
            }

            dispatchSchemaChangeEventInternal(event);
        }
    }

    @Override
    protected void handleTruncateEvent(LogMinerEventRow event) throws InterruptedException {
        try {
            final Table table = getTableForDataEvent(event);
            if (table != null) {
                LOGGER.debug("Dispatching TRUNCATE event for table '{}' with SCN {}", table.id(), event.getScn());
                enqueueEvent(event, new TruncateEvent(event, parseTruncateEvent(event)));
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to process truncate event", e);
            getMetrics().incrementWarningCount();
        }
    }

    @Override
    protected boolean isDispatchAllowedForDataChangeEvent(LogMinerEventRow event) {
        if (event.isRollbackFlag()) {
            // There is a use case where a constraint violation will result in a DML event being
            // written to the redo log subsequently followed by another DML event that is marked
            // with a rollback flag to indicate that the prior event should be omitted. In this
            // use case, the transaction can still be committed, so we need to manually rollback
            // the previous DML event when this use case occurs.
            removeEventWithRowId(event);
            return false;
        }
        return true;
    }

    @Override
    protected void handleReplicationMarkerEvent(LogMinerEventRow event) {
        // GoldenGate creates replication markers in the redo logs periodically and these entries can lead to
        // the construction of a transaction in the buffer that never has a COMMIT or ROLLBACK. When this is
        // done by Oracle, we should automatically discard the transaction from the buffer to avoid the low
        // watermark from advancing safely.
        final String transactionId = event.getTransactionId();
        final Transaction transaction = getTransactionCache().getTransaction(transactionId);
        if (transaction != null) {
            LOGGER.debug("Skipping GoldenGate replication marker for transaction {} with SCN {}", transactionId, event.getScn());
            getTransactionCache().removeTransactionEvents(transaction);
            getTransactionCache().removeTransaction(transaction);
        }
        // It should not exist in this cache, but in case.
        getTransactionCache().removeAbandonedTransaction(transactionId);
    }

    @Override
    protected boolean isNoDataProcessedInBatchAndAtEndOfArchiveLogs() {
        return !getMetrics().getBatchMetrics().hasJdbcRows();
    }

    /**
     * Calculates the new SCN that will be used as the resume position for the next iteration.
     *
     * @param startScn the current iteration's lower bounds system change number
     * @param endScn the current iteration's upper bounds system change number
     * @param maxCommittedScn the current maximum scn committed
     * @return the new resume position change number for the next iteration
     * @throws InterruptedException if the thread is interrupted
     */
    private Scn calculateNewStartScn(Scn startScn, Scn endScn, Scn maxCommittedScn) throws InterruptedException {

        if (!getBatchMetrics().hasJdbcRows()) {
            // When no rows are processed, don't advance the SCN
            return startScn;
        }

        // Cleanup caches based on current state of the transaction cache
        final Scn minCacheScn;
        final Instant minCacheScnChangeTime;
        final Optional<LogMinerTransactionCache.ScnDetails> eldestScnDetails = getTransactionCache().getEldestTransactionScnDetailsInCache();
        if (eldestScnDetails.isPresent()) {
            minCacheScn = eldestScnDetails.get().scn();
            minCacheScnChangeTime = eldestScnDetails.get().changeTime();
        }
        else {
            minCacheScn = Scn.NULL;
            minCacheScnChangeTime = null;
        }

        if (!minCacheScn.isNull()) {
            abandonTransactions(getConfig().getLogMiningTransactionRetention());

            getProcessedTransactionsCache().removeIf(entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
            getSchemaChangesCache().removeIf(entry -> Scn.valueOf(entry.getKey()).compareTo(minCacheScn) < 0);
        }
        else {
            getSchemaChangesCache().removeIf(e -> true);
        }

        if (getConfig().isLobEnabled()) {
            if (getTransactionCache().isEmpty() && !maxCommittedScn.isNull()) {
                getOffsetContext().setScn(maxCommittedScn);
                getEventDispatcher().dispatchHeartbeatEvent(getPartition(), getOffsetContext());
            }
            else {
                if (!minCacheScn.isNull()) {
                    getProcessedTransactionsCache().removeIf(entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
                    getOffsetContext().setScn(minCacheScn.subtract(Scn.valueOf(1)));
                    getEventDispatcher().dispatchHeartbeatEvent(getPartition(), getOffsetContext());
                }
            }
            return getOffsetContext().getScn();
        }
        else {

            if (!lastProcessedScn.isNull() && lastProcessedScn.compareTo(endScn) < 0) {
                // If the last processed SCN is before the endScn we need to use the last processed SCN as the
                // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
                endScn = lastProcessedScn;
            }

            getOffsetContext().setScn(minCacheScn.isNull() ? endScn : minCacheScn.subtract(Scn.valueOf(1)));
            getMetrics().setOldestScnDetails(minCacheScn, minCacheScnChangeTime);
            getMetrics().setOffsetScn(getOffsetContext().getScn());

            // optionally dispatch a heartbeat event
            getEventDispatcher().dispatchHeartbeatEvent(getPartition(), getOffsetContext());

            return endScn;
        }
    }

    /**
     * Calculates the smallest system change number currently in the transaction cache, if any exist.
     *
     * @return the smallest cached scn or {@link Scn#NULL} if the cache is empty
     */
    private Scn calculateSmallestScn() {
        return getTransactionCache().getEldestTransactionScnDetailsInCache()
                .map(scnDetails -> {
                    getMetrics().setOldestScnDetails(scnDetails.scn(), scnDetails.changeTime());
                    return scnDetails.scn();
                })
                .orElseGet(() -> {
                    getMetrics().setOldestScnDetails(Scn.valueOf(-1), null);
                    return Scn.NULL;
                });
    }

    /**
     * Removes a change from the event's current transaction that matches the row identifier information of
     * the supplied event. This is necessary for handling constraint violation or savepoint rollbacks.
     *
     * @param row the event, should not be {@code null}
     */
    private void removeEventWithRowId(LogMinerEventRow row) {
        final Transaction transaction = getTransactionCache().getTransaction(row.getTransactionId());
        if (transaction != null) {
            if (removeTransactionEventWithRowId(transaction, row)) {
                return;
            }
            Loggings.logWarningAndTraceRecord(LOGGER, row,
                    "Cannot apply undo change in transaction '{}' with SCN '{}' on table '{}' since event with row-id {} was not found.",
                    row.getTransactionId(), row.getScn(), row.getTableId(), row.getRowId());
        }
        else if (row.getTransactionId().endsWith(NO_SEQUENCE_TRX_ID_SUFFIX)) {
            // This means that Oracle LogMiner found an event that should be undone but its corresponding
            // undo entry was read in a prior mining session and the transaction's sequence could not be
            // resolved.
            final String prefix = row.getTransactionId().substring(0, 8);
            LOGGER.debug("Undo change refers to a transaction that has no explicit sequence, '{}'", row.getTransactionId());
            LOGGER.debug("Checking all transactions with prefix '{}'", prefix);

            if (getTransactionCache().streamTransactionsAndReturn(
                    stream -> stream.filter(t -> t.getTransactionId().startsWith(prefix))
                            .anyMatch(t -> removeTransactionEventWithRowId(t, row)))) {
                return;
            }

            Loggings.logWarningAndTraceRecord(LOGGER, row,
                    "Cannot apply undo change in transaction '{}' with SCN '{}' on table '{}' since event with row-id {} was not found.",
                    row.getTransactionId(), row.getScn(), row.getTableId(), row.getRowId());
        }
        else if (!getConfig().isLobEnabled()) {
            Loggings.logWarningAndTraceRecord(LOGGER, row,
                    "Cannot apply undo change with SCN '{}' on table '{}' since transaction '{}' was not found.",
                    row.getScn(), row.getTableId(), row.getTransactionId());
        }
        else {
            // While the code should never get here, log a warning if it does.
            Loggings.logWarningAndTraceRecord(LOGGER, row,
                    "Failed to apply undo change with SCN '{}' on table '{}' in transaction '{}' with row-id '{}'",
                    row.getScn(), row.getTableId(), row.getTransactionId(), row.getRowId());
        }
    }

    /**
     * For the specified transaction and change event, removes the latest event from the event cache that
     * matches the transaction and the change event's row identifier values.
     *
     * @param transaction the transaction, should not be {@code null}
     * @param row the event, should not be {@code null}
     * @return true if an event was found and undone, false otherwise
     */
    private boolean removeTransactionEventWithRowId(Transaction transaction, LogMinerEventRow row) {
        if (getTransactionCache().removeTransactionEventWithRowId(transaction, row.getRowId())) {
            // This metric won't necessarily be accurate when LOB is enabled, it will scale based on the
            // number of times a given transaction is re-mined.
            getMetrics().increasePartialRollbackCount();
            getBatchMetrics().partialRollbackObserved();

            Loggings.logDebugAndTraceRecord(LOGGER, row,
                    "Undo change on table '{}' applied to transaction event with row-id '{}'",
                    row.getTableId(), row.getRowId());
            return true;
        }
        return false;
    }

    /**
     * Perform necessary cache cleanup actions after the given transaction was removed.
     *
     * @param transaction the transaction that was removed, should not be {@code null}
     * @param isAbandoned true if the transaction was removed because it was abandoned, false otherwise
     */
    private void cleanupAfterTransactionRemovedFromCache(Transaction transaction, boolean isAbandoned) {
        if (isAbandoned) {
            getTransactionCache().abandon(transaction);
        }
        else {
            getTransactionCache().removeAbandonedTransaction(transaction.getTransactionId());
        }
        getTransactionCache().removeTransactionEvents(transaction);
    }

    @Override
    protected boolean hasEventBeenProcessed(LogMinerEventRow event) {
        final String transactionId = event.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.debug("Transaction {} has been seen by connector, skipped.", transactionId);
            return true;
        }
        else if (getTransactionCache().isAbandoned(transactionId)) {
            LOGGER.debug("Event for abandoned transaction {}, skipped.", transactionId);
            return true;
        }

        return isEventIncludedInSnapshot(event);
    }

    /**
     * Checks whether the given transaction should be skipped at commit time by inspecting various
     * criteria that may not have been applied at query time during to the configured query filter
     * mode.
     *
     * @param transaction the transaction, should not be {@code null}
     * @return true if the transaction should be skipped and not dispatched, false otherwise
     */
    private boolean isTransactionSkippedAtCommit(Transaction transaction) {
        // todo: can this be moved to earlier in the processing loop to avoid buffering?
        return transaction != null && (isUserNameSkipped(transaction.getUserName()) || isClientIdSkipped(transaction.getClientId()));
    }

    /**
     * Performs finalization steps for a transaction when its committed or rolled back.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     * @param eventScn the event's system change number, should not be {@code null}
     * @param rollbackEvent true if the transaction was rolled back, false if it was committed
     */
    private void finalizeTransaction(String transactionId, Scn eventScn, boolean rollbackEvent) {
        if (rollbackEvent) {
            final Transaction transaction = getTransactionCache().getTransaction(transactionId);
            if (transaction != null) {
                getTransactionCache().removeTransactionEvents(transaction);
                getTransactionCache().removeTransaction(transaction);
            }
        }

        getTransactionCache().removeAbandonedTransaction(transactionId);

        if (getConfig().isLobEnabled()) {
            getProcessedTransactionsCache().put(transactionId, eventScn.toString());
        }
    }

    /**
     * Checks whether the lower system change number in the offsets can be advanced on a schema change.
     *
     * @param row the event, should not be {@code null}
     * @return true if the offset scn can be advanced, false otherwise
     */
    private boolean canAdvanceLowerScnBoundaryOnSchemaChange(LogMinerEventRow row) {
        final int cacheSize = getTransactionCache().getTransactionCount();
        if (cacheSize == 0) {
            // The DDL isn't wrapped in a transaction, fast-forward the lower boundary
            return true;
        }
        else if (cacheSize == 1) {
            // The row's transaction is the current and only active transaction.
            return getTransactionCache().streamTransactionsAndReturn(stream -> stream.map(Transaction::getTransactionId)
                    .allMatch(trxId -> trxId.equals(row.getTransactionId())));
        }
        return false;
    }

    @Override
    protected void enqueueEvent(LogMinerEventRow event, LogMinerEvent dispatchedEvent) throws InterruptedException {
        final String transactionId = event.getTransactionId();

        Transaction transaction = getTransactionCache().getTransaction(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} is not in cache, creating.", transactionId);
            transaction = transactionFactory.createTransaction(event);
            getTransactionCache().addTransaction(transaction);
        }

        final int eventId = transaction.getNextEventId();
        if (!getTransactionCache().containsTransactionEvent(transaction, eventId)) {
            // Add new event at eventId offset
            LOGGER.trace("Transaction {}, adding event reference at key {}", transactionId, transaction.getEventId(eventId));
            getTransactionCache().addTransactionEvent(transaction, eventId, dispatchedEvent);
            getMetrics().calculateLagFromSource(event.getChangeTime());
        }

        // When using Infinispan, this extra put is required so that the state is properly synchronized
        getTransactionCache().syncTransaction(transaction);
        getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
    }

    /**
     * Check whether the transaction's cached event count exceeds the configured event threshold limit.
     *
     * @param transaction the transaction, should not be {@code null}
     * @return true if the transaction's event count exceeds the threshold limit, false otherwise
     */
    private boolean isTransactionOverEventThreshold(Transaction transaction) {
        if (getConfig().getLogMiningBufferTransactionEventsThreshold() <= 0) {
            return false;
        }
        return getTransactionEventCount(transaction) >= getConfig().getLogMiningBufferTransactionEventsThreshold();
    }

    /**
     * Abandons the given transaction with an event count that exceeds the configured threshold limit.
     *
     * @param transaction the transaction, should not be {@code null}
     */
    private void abandonTransactionOverEventThreshold(Transaction transaction) {
        LOGGER.warn("Transaction {} exceeds maximum allowed number of events, transaction will be abandoned.", transaction.getTransactionId());
        getMetrics().incrementWarningCount();
        getTransactionCache().getAndRemoveTransaction(transaction.getTransactionId());
        cleanupAfterTransactionRemovedFromCache(transaction, true);
        getMetrics().incrementOversizedTransactionCount();
    }

    /**
     * Abandons all transactions that have been cached longer than the retention period based on the last
     * processed event's system change number. Passing {@link Duration#ZERO} skips the abandon step.
     *
     * @param retention the retention duration, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    @VisibleForTesting
    protected void abandonTransactions(Duration retention) throws InterruptedException {
        if (!Duration.ZERO.equals(retention)) {
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(getConnection(), retention);
            if (lastScnToAbandonTransactions.isPresent()) {
                Scn thresholdScn = lastScnToAbandonTransactions.get();
                Scn smallestScn = getTransactionCache().getEldestTransactionScnDetailsInCache().map(LogMinerTransactionCache.ScnDetails::scn).orElse(Scn.NULL);
                if (!smallestScn.isNull() && thresholdScn.compareTo(smallestScn) >= 0) {

                    Map<String, Transaction> abandoned = getTransactionCache().streamTransactionsAndReturn(stream -> stream
                            .filter(t -> t.getStartScn().compareTo(thresholdScn) <= 0)
                            .collect(Collectors.toMap(Transaction::getTransactionId, t -> t)));

                    boolean first = true;
                    for (Map.Entry<String, Transaction> entry : abandoned.entrySet()) {
                        if (first) {
                            LOGGER.warn("All transactions with SCN <= {} will be abandoned.", thresholdScn);
                            first = false;
                        }
                        String key = entry.getKey();
                        Transaction value = entry.getValue();

                        LOGGER.warn("Transaction {} (start SCN {}, change time {}, redo thread {}, {} events{}) is being abandoned.",
                                key, value.getStartScn(), value.getChangeTime(), value.getRedoThreadId(),
                                value.getNumberOfEvents(), getLoggedAbandonedTransactionTableNames(value));

                        cleanupAfterTransactionRemovedFromCache(value, true);

                        getTransactionCache().removeTransaction(value);
                        getMetrics().addAbandonedTransactionId(key);
                    }

                    getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("List of transactions in the cache before transactions being abandoned: [{}]",
                                String.join(",", abandoned.keySet()));

                        getTransactionCache().transactions(stream -> {
                            LOGGER.debug("List of transactions in the cache after transactions begin abandoned: [{}]",
                                    stream.map(Transaction::getTransactionId).collect(Collectors.joining(",")));
                        });
                    }

                    // Update the oldest scn metric are transaction abandonment
                    getTransactionCache().getEldestTransactionScnDetailsInCache().ifPresentOrElse(
                            scnDetails -> getMetrics().setOldestScnDetails(scnDetails.scn(), scnDetails.changeTime()),
                            () -> getMetrics().setOldestScnDetails(Scn.NULL, null));

                    getOffsetContext().setScn(thresholdScn);
                }
                getEventDispatcher().dispatchHeartbeatEvent(getPartition(), getOffsetContext());
            }
        }
    }

    /**
     * Computes the log entry for the specified transaction that includes all affected tables with changes that
     * will be discarded by the connector.
     *
     * @param transaction the transaction being abandoned, should not be {@code null}
     * @return the log entry text, may be empty but never {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    private String getLoggedAbandonedTransactionTableNames(Transaction transaction) throws InterruptedException {
        if (ABANDONED_DETAILS_LOGGER.isDebugEnabled()) {
            final Set<String> tableNames = new HashSet<>();
            getTransactionCache().forEachEvent(transaction, event -> {
                tableNames.add(event.getTableId().identifier());
                return true;
            });
            return String.format(", %d tables [%s]", tableNames.size(), String.join(",", tableNames));
        }
        return "";
    }

    /**
     * Computes the system change number boundary for abandoning transactions.
     * <p>
     * This method takes the {@link #lastProcessedScn} and the provided {@code retention} and subtracts
     * the retention from the last processed system change number. This will provide the position where
     * all transactions prior to that boundary are abandoned.
     *
     * @param connection the database connection, should not be {@code null}
     * @param retention the retention period, should not be {@code null}
     * @return an optional system change number if one was computed, or empty if the computation failed
     */
    private Optional<Scn> getLastScnToAbandon(OracleConnection connection, Duration retention) {
        try {
            if (lastProcessedScn.isNull()) {
                return Optional.empty();
            }
            BigInteger scnToAbandon = connection.singleOptionalValue(
                    SqlUtils.getScnByTimeDeltaQuery(lastProcessedScn, retention),
                    rs -> rs.getBigDecimal(1).toBigInteger());
            return Optional.of(new Scn(scnToAbandon));
        }
        catch (SQLException e) {
            // This can happen when the last processed SCN has aged out of the UNDO_RETENTION.
            // In this case, we use a fallback in order to calculate the SCN based on the
            // change times in the transaction cache.
            if (lastProcessedScnChangeTime != null) {
                final Scn calculatedLastScn = getLastScnToAbandonFallbackByTransactionChangeTime(retention);
                if (!calculatedLastScn.isNull()) {
                    return Optional.of(calculatedLastScn);
                }
            }

            // Both SCN database calculation and fallback failed, log error.
            LOGGER.error("Cannot fetch SCN {} by given duration to calculate SCN to abandon", lastProcessedScn, e);
            getMetrics().incrementErrorCount();
            return Optional.empty();
        }
    }

    /**
     * A fallback approach to calculating the system change number boundary for abandoning transactions.
     * <p>
     * This approach walks the transaction cache and computes the age of each transaction since the last processed
     * event the connector saw. If the age exceeds the retention policy, we determine what is the maximum
     * starting position for all transactions.
     *
     * @param retention the retention, should not be {@code null}
     * @return the system change number boundary for abandoning transactions or {@link Scn#NULL} if cache is empty
     */
    private Scn getLastScnToAbandonFallbackByTransactionChangeTime(Duration retention) {
        LOGGER.debug("Getting abandon SCN breakpoint based on change time {} (retention {} minutes).",
                lastProcessedScnChangeTime, retention.toMinutes());

        return getTransactionCache().streamTransactionsAndReturn(stream -> stream.filter(t -> {
            final Instant changeTime = t.getChangeTime();
            final long diffMinutes = Duration.between(lastProcessedScnChangeTime, changeTime).abs().toMinutes();

            // We either now will capture the transaction's SCN because it is the first detected transaction
            // outside the configured retention period or the transaction has a start SCN that is more recent
            // than the current calculated SCN but is still outside the configured retention period.
            LOGGER.debug("Transaction {} with SCN {} started at {}, age is {} minutes.",
                    t.getTransactionId(), t.getStartScn(), changeTime, diffMinutes);
            return diffMinutes > 0 && diffMinutes > retention.toMinutes();
        })
                .max(Comparator.comparing(Transaction::getStartScn))
                .map(Transaction::getStartScn)
                .orElse(Scn.NULL));
    }

    /**
     * Logs all active transactions.
     */
    private void logActiveTransactions() {
        if (LOGGER.isDebugEnabled() && !getTransactionCache().isEmpty()) {
            // This is wrapped in try-with-resources specifically for Infinispan performance
            cacheProvider.getTransactionCache().transactions(transactions -> {
                LOGGER.debug("All active transactions: {}",
                        transactions.map(t -> t.getTransactionId() + " (" + t.getStartScn() + ")")
                                .collect(Collectors.joining(",")));
            });
        }
    }
}
