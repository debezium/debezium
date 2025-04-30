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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
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
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.RacCommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.ReadOnlyLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.ExtendedStringParser;
import io.debezium.connector.oracle.logminer.parser.LobWriteParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerColumnResolverDmlParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.connector.oracle.logminer.parser.XmlBeginParser;
import io.debezium.connector.oracle.logminer.parser.XmlWriteParser;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
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

    private final LogMinerDmlParser dmlParser;
    private final LogMinerColumnResolverDmlParser reconstructColumnDmlParser;
    private final SelectLobParser selectLobParser;
    private final ExtendedStringParser extendedStringParser;
    private final XmlBeginParser xmlBeginParser;
    private final TableFilter tableFilter;
    private final String queryString;
    private final CacheProvider<Transaction> cacheProvider;
    private final TransactionFactory<Transaction> transactionFactory;

    private int stuckCount = 0;
    private Scn currentOffsetScn = Scn.NULL;
    private Map<Integer, Scn> currentOffsetCommitScns = new HashMap<>();
    private Instant lastProcessedScnChangeTime = null;
    private Scn lastProcessedScn = Scn.NULL;
    private boolean sequenceUnavailable = false;

    public BufferedLogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                                      OracleConnection jdbcConnection,
                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                      ErrorHandler errorHandler,
                                                      Clock clock,
                                                      OracleDatabaseSchema schema,
                                                      Configuration jdbcConfig,
                                                      LogMinerStreamingChangeEventSourceMetrics streamingMetrics) {
        super(connectorConfig, jdbcConnection, dispatcher, errorHandler, clock, schema, jdbcConfig, streamingMetrics);

        this.dmlParser = new LogMinerDmlParser(connectorConfig);
        this.reconstructColumnDmlParser = new LogMinerColumnResolverDmlParser(connectorConfig);
        this.selectLobParser = new SelectLobParser();
        this.extendedStringParser = new ExtendedStringParser();
        this.xmlBeginParser = new XmlBeginParser();
        this.queryString = new BufferedLogMinerQueryBuilder(connectorConfig).getQuery();
        this.tableFilter = connectorConfig.getTableFilters().dataCollectionFilter();
        this.cacheProvider = createCacheProvider(connectorConfig);
        this.transactionFactory = createTransactionFactory(connectorConfig);
    }

    @Override
    protected void executeLogMiningStreaming(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext)
            throws Exception {

        try (LogWriterFlushStrategy flushStrategy = resolveFlushStrategy()) {

            Scn startScn = getOffsetContext().getScn();
            Scn endScn = Scn.NULL;

            Stopwatch watch = Stopwatch.accumulating().start();
            int miningStartAttempts = 1;

            prepareLogsForMining(false, startScn);

            while (context.isRunning()) {

                // Check if we should break when using archive log only mode
                if (isArchiveLogOnlyModeAndScnIsNotAvailable(context, startScn)) {
                    break;
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

                // This is a small window where when archive log only mode has completely caught up to the last
                // record in the archive logs that both the lower and upper values are identical. In this use
                // case we want to pause and restart the loop waiting for a new archive log before proceeding.
                if (getConfig().isArchiveLogOnlyMode() && startScn.equals(endScn)) {
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
                    startScn = process(context, startScn, endScn);

                    getMetrics().setLastBatchProcessingDuration(Duration.between(batchStartTime, Instant.now()));
                }
                else {
                    miningStartAttempts++;
                }

                captureJdbcSessionMemoryStatistics();

                pauseBetweenMiningSessions();

                if (context.isPaused()) {
                    LOGGER.info("Streaming will now pause");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed");
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
    protected Scn process(ChangeEventSourceContext context, Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        getBatchMetrics().reset();

        try (PreparedStatement statement = createQueryStatement()) {
            LOGGER.debug("Fetching results for SCN [{}, {}]", startScn, endScn);
            statement.setFetchSize(getConfig().getQueryFetchSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, startScn.toString());
            statement.setString(2, endScn.toString());

            final Instant queryStart = Instant.now();
            try (ResultSet resultSet = statement.executeQuery()) {
                getMetrics().setLastDurationOfFetchQuery(Duration.between(queryStart, Instant.now()));

                final Instant startProcessTime = Instant.now();
                while (context.isRunning() && hasNextWithMetricsUpdate(resultSet)) {
                    getBatchMetrics().rowObserved();
                    getBatchMetrics().rowProcessed();
                    processEvent(LogMinerEventRow.fromResultSet(resultSet, getConfig().getCatalogName()), context);
                }

                getBatchMetrics().updateStreamingMetrics();

                if (getBatchMetrics().hasProcessedAnyTransactions()) {
                    warnPotentiallyStuckScn(currentOffsetScn, currentOffsetCommitScns);

                    currentOffsetScn = getOffsetContext().getScn();
                    if (getOffsetContext().getCommitScn() != null) {
                        currentOffsetCommitScns = getOffsetContext().getCommitScn().getCommitScnForAllRedoThreads();
                    }
                }

                LOGGER.debug("{}.", getBatchMetrics());
                LOGGER.debug("Processed in {} ms. Lag: {}. Offset SCN: {}, Offset Commit SCN: {}, Active Transactions: {}, Sleep: {}",
                        Duration.between(startProcessTime, Instant.now()),
                        getMetrics().getLagFromSourceInMilliseconds(),
                        getOffsetContext().getScn(),
                        getOffsetContext().getCommitScn(),
                        getMetrics().getNumberOfActiveTransactions(),
                        getMetrics().getSleepTimeInMilliseconds());

                if (getMetrics().getNumberOfActiveTransactions() > 0 && LOGGER.isDebugEnabled()) {
                    // This is wrapped in try-with-resources specifically for Infinispan performance
                    cacheProvider.getTransactionCache().transactions(transactions -> {
                        LOGGER.debug("All action transactions: {}",
                                transactions.map(t -> t.getTransactionId() + " (" + t.getStartScn() + ")")
                                        .collect(Collectors.joining(",")));
                    });
                }

                if (!getBatchMetrics().hasProcessedRows()) {
                    // When no rows are processed, don't advance the SCN
                    return startScn;
                }
                else {
                    return calculateNewStartScn(endScn, getOffsetContext().getCommitScn().getMaxCommittedScn());
                }
            }
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

    @VisibleForTesting
    protected void processEvent(LogMinerEventRow event, ChangeEventSourceContext context) throws SQLException, InterruptedException {
        final String transactionId = event.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.debug("Transaction {} has been seen by connector, skipped.", transactionId);
            return;
        }

        if (!event.getEventType().equals(EventType.MISSING_SCN)) {
            lastProcessedScn = event.getScn();
            lastProcessedScnChangeTime = event.getChangeTime();
        }
        // filter out all events that are captured as part of the initial snapshot
        if (event.getScn().compareTo(getOffsetContext().getSnapshotScn()) < 0) {
            Map<String, Scn> snapshotPendingTransactions = getOffsetContext().getSnapshotPendingTransactions();
            if (snapshotPendingTransactions == null || !snapshotPendingTransactions.containsKey(event.getTransactionId())) {
                LOGGER.debug("Skipping event {} (SCN {}) because it is already encompassed by the initial snapshot", event.getEventType(), event.getScn());
                return;
            }
        }

        // Check whether the row has a table reference and if so, is the reference included by the filter.
        // If the reference isn't included, the row will be skipped entirely.
        if (event.getTableId() != null) {
            if (LogWriterFlushStrategy.isFlushTable(event.getTableId(), getConfig().getJdbcConfig().getUser(), getConfig().getLogMiningFlushTableName())) {
                LOGGER.trace("Skipped change associated with flush table '{}'", event.getTableId());
                return;
            }

            // DDL events get filtered inside the DDL handler
            // We do the non-DDL ones here to cover multiple switch handlers in one place.
            if (!EventType.DDL.equals(event.getEventType()) && !tableFilter.isIncluded(event.getTableId())) {
                if (isNonIncludedTableSkipped(event)) {
                    LOGGER.trace("Skipping change associated with table '{}' which does not match filters.", event.getTableId());
                    return;
                }
            }
        }

        switch (event.getEventType()) {
            case MISSING_SCN -> handleMissingScnEvent(event);
            case START -> handleStartEvent(event);
            case COMMIT -> handleCommitEvent(event, context);
            case ROLLBACK -> handleRollbackEvent(event);
            case DDL -> handleSchemaChangeEvent(event);
            case INSERT, UPDATE, DELETE -> handleDataChangeEvent(event);
            case REPLICATION_MARKER -> handleReplicationMarkerEvent(event);
            case UNSUPPORTED -> handleUnsupportedEvent(event);
            case SELECT_LOB_LOCATOR -> handleSelectLobLocatorEvent(event);
            case LOB_WRITE -> handleLobWriteEvent(event);
            case LOB_ERASE -> handleLobEraseEvent(event);
            case XML_BEGIN -> handleXmlBeginEvent(event);
            case XML_WRITE -> handleXmlWriteEvent(event);
            case XML_END -> handleXmlEndEvent(event);
            case EXTENDED_STRING_BEGIN -> handleExtendedStringBeginEvent(event);
            case EXTENDED_STRING_WRITE -> handleExtendedStringWriteEvent(event);
            case EXTENDED_STRING_END -> handleExtendedStringEndEvent(event);
            default -> Loggings.logDebugAndTraceRecord(LOGGER, event, "Skipped event {}", event.getEventType());
        }
    }

    private void handleMissingScnEvent(LogMinerEventRow event) {
        LOGGER.warn("Missing SCN detected. {}", event);
    }

    private void handleStartEvent(LogMinerEventRow event) {
        final String transactionId = event.getTransactionId();
        final Transaction transaction = getTransactionCache().getTransaction(transactionId);
        if (transaction == null && !isRecentlyProcessed(transactionId)) {
            getTransactionCache().addTransaction(transactionFactory.createTransaction(event));
            getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
        }
        else if (transaction != null && !isRecentlyProcessed(transactionId)) {
            LOGGER.trace("Transaction {} is not yet committed and START event detected.", transactionId);
            getTransactionCache().resetTransactionToStart(transaction);
        }
    }

    private void handleCommitEvent(LogMinerEventRow row, ChangeEventSourceContext context) throws InterruptedException {
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
            handleCommitNotFoundInBuffer(row);
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
            final boolean skipEvents = isTransactionSkipped(transaction);
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
                    if (!context.isRunning()) {
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

        getMetrics().calculateLagFromSource(row.getChangeTime());

        if (transaction != null) {
            finalizeTransactionCommit(transactionId, commitScn);
            cleanupAfterTransactionRemovedFromCache(transaction, false);
            getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
        }

        getMetrics().incrementCommittedTransactionCount();
        getMetrics().setCommitScn(commitScn);
        getMetrics().setOffsetScn(getOffsetContext().getScn());
        getMetrics().setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    private void handleRollbackEvent(LogMinerEventRow row) {
        if (getTransactionCache().containsTransaction(row.getTransactionId())) {
            LOGGER.debug("Transaction {} was rolled back.", row.getTransactionId());
            finalizeTransactionRollback(row.getTransactionId(), row.getScn());
            getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
        }
        else {
            LOGGER.debug("Transaction {} not found in cache, no events to rollback.", row.getTransactionId());
            handleRollbackNotFoundInBuffer(row);
        }
        getMetrics().incrementRolledBackTransactionCount();
        getMetrics().addRolledBackTransactionId(row.getTransactionId());
        getBatchMetrics().rollbackObserved();
    }

    private void handleSchemaChangeEvent(LogMinerEventRow row) throws InterruptedException {
        if (row.getTableId() != null) {
            boolean isExcluded = getConfig().getLogMiningSchemaChangesUsernameExcludes()
                    .stream()
                    .anyMatch(userName -> userName.equalsIgnoreCase(row.getUserName()));
            if (isExcluded) {
                LOGGER.trace("User '{}' is in schema change exclusions, DDL '{}' skipped.", row.getUserName(), row.getRedoSql());
                return;
            }
            else if (row.getInfo() != null && row.getInfo().startsWith("INTERNAL DDL")) {
                // Internal DDL operations are skipped.
                LOGGER.trace("Internal DDL '{}' skipped", row.getRedoSql());
                return;
            }

            if (getSchema().storeOnlyCapturedTables() && !tableFilter.isIncluded(row.getTableId())) {
                LOGGER.trace("Skipping DDL associated with table '{}', schema history only stores included tables only.", row.getTableId());
                return;
            }
        }

        if (hasSchemaChangeBeenSeen(row)) {
            LOGGER.trace("DDL: Scn {}, SQL '{}' has already been processed, skipped.", row.getScn(), row.getRedoSql());
            return;
        }

        if (getOffsetContext().getCommitScn().hasEventScnBeenHandled(row)) {
            final Scn commitScn = getOffsetContext().getCommitScn().getCommitScnForRedoThread(row.getThread());
            LOGGER.trace("DDL: SQL '{}' skipped with {} (SCN) <= {} (commit SCN for redo thread {})",
                    row.getRedoSql(), row.getScn(), commitScn, row.getThread());
            return;
        }

        LOGGER.trace("DDL: '{}' {}", row.getRedoSql(), row);
        if (row.getTableName() != null) {
            getBatchMetrics().schemaChangeObserved();
            final TableId tableId = row.getTableId();

            if (canAdvanceLowerScnBoundary(row)) {
                LOGGER.debug("Schema change advanced offset SCN to {}", row.getScn());
                getOffsetContext().setScn(row.getScn());
            }

            // Should always advance the commit SCN point with schema changes
            LOGGER.debug("Schema change advanced offset commit SCN to {} for thread {}", row.getScn(), row.getThread());
            getOffsetContext().getCommitScn().recordCommit(row);

            getOffsetContext().setEventScn(row.getScn());
            getOffsetContext().setRedoThread(row.getThread());
            getOffsetContext().setRsId(row.getRsId());
            getOffsetContext().setRowId("");

            if (getConfig().isLobEnabled()) {
                getSchemaChangesCache().put(row.getScn().toString(), row.getTableId().identifier());
            }

            getEventDispatcher().dispatchSchemaChangeEvent(getPartition(),
                    getOffsetContext(),
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            getConfig(),
                            getPartition(),
                            getOffsetContext(),
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            row.getObjectId(),
                            // ALTER TABLE does not populate the data object id, pass object id on purpose
                            row.getObjectId(),
                            row.getRedoSql(),
                            getSchema(),
                            row.getChangeTime(),
                            getMetrics(),
                            () -> processTruncateEvent(row)));

            if (isUsingHybridStrategy()) {
                // Removes table from the column-based DML parser cache
                // It will be refreshed on the next DML event that requires special parsing
                reconstructColumnDmlParser.removeTableFromCache(tableId);
            }
        }
    }

    private void processTruncateEvent(LogMinerEventRow row) {
        LOGGER.debug("Handling truncate event");

        try {
            // Truncate event is being treated as a DML.
            Table table = getTableForDataEvent(row);
            if (table == null) {
                return;
            }
        }
        catch (SQLException | InterruptedException e) {
            LOGGER.warn("Failed to process truncate event.", e);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> {
            final LogMinerDmlEntry dmlEntry = LogMinerDmlEntryImpl.forValuelessDdl();
            dmlEntry.setObjectName(row.getTableName());
            dmlEntry.setObjectOwner(row.getTablespaceName());
            return new TruncateEvent(row, dmlEntry);
        });
    }

    private void handleSelectLobLocatorEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, SEL_LOB_LOCATOR '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.debug("SEL_LOB_LOCATOR: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("SEL_LOB_LOCATOR for table '{}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(),
                row,
                () -> {
                    final LogMinerDmlEntry dmlEntry = selectLobParser.parse(row.getRedoSql(), table);
                    dmlEntry.setObjectName(row.getTableName());
                    dmlEntry.setObjectOwner(row.getTablespaceName());

                    return new SelectLobLocatorEvent(row,
                            dmlEntry,
                            selectLobParser.getColumnName(),
                            selectLobParser.isBinary());
                });

        getMetrics().incrementTotalChangesCount();
    }

    private void handleLobWriteEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, LOB_WRITE scn={}, tableId={} skipped", row.getScn(), row.getTableId());
            return;
        }

        LOGGER.debug("LOB_WRITE: scn={}, tableId={}, changeTime={}, transactionId={}",
                row.getScn(), row.getTableId(), row.getChangeTime(), row.getTransactionId());

        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("LOB_WRITE for table '{}' is not known, skipped", tableId);
            return;
        }

        if (row.getRedoSql() != null) {
            addToTransaction(row.getTransactionId(), row, () -> {
                final LobWriteParser.LobWrite parsedEvent = LobWriteParser.parse(row.getRedoSql());
                return new LobWriteEvent(row, parsedEvent.data(), parsedEvent.offset(), parsedEvent.length());
            });
        }
    }

    private void handleLobEraseEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, LOB_ERASE '{}' skipped", row);
            return;
        }

        LOGGER.debug("LOB_ERASE: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("LOB_ERASE for table '{}' is not known, skipped", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> new LobEraseEvent(row));
    }

    private void handleExtendedStringBeginEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, 32K_START '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.debug("32K_BEGIN: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("32K_BEGIN for table '{}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(),
                row,
                () -> {
                    final LogMinerDmlEntry dmlEntry = extendedStringParser.parse(row.getRedoSql(), table);
                    dmlEntry.setObjectName(row.getTableName());
                    dmlEntry.setObjectOwner(row.getTablespaceName());

                    final String columnName = extendedStringParser.getColumnName();
                    return new ExtendedStringBeginEvent(row, dmlEntry, columnName);
                });

        getMetrics().incrementTotalChangesCount();
    }

    private void handleExtendedStringWriteEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, 32K_WRITE '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.debug("32K_WRITE: scn={}, tableId={}, changeTime={}, transactionId={}",
                row.getScn(), row.getTableId(), row.getChangeTime(), row.getTransactionId());

        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("32K_WRITE for table '{}' is not known, skipped", tableId);
            return;
        }

        if (row.getRedoSql() != null) {
            addToTransaction(row.getTransactionId(), row, () -> {
                final String data = parseExtendedStringWriteSql(row.getRedoSql());
                return new ExtendedStringWriteEvent(row, data);
            });
        }
    }

    private void handleExtendedStringEndEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, 32K_END '{}' skipped.", row.getRedoSql());
        }
    }

    private void handleXmlBeginEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, XML_BEGIN '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.trace("XML_BEGIN: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("XML_BEGIN for table '{}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(),
                row,
                () -> {
                    final LogMinerDmlEntry dmlEntry = xmlBeginParser.parse(row.getRedoSql(), table);
                    dmlEntry.setObjectName(row.getTableName());
                    dmlEntry.setObjectOwner(row.getTablespaceName());
                    return new XmlBeginEvent(row, dmlEntry, xmlBeginParser.getColumnName());
                });
    }

    private void handleXmlWriteEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, XML_WRITE '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.trace("XML_WRITE: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("XML_WRITE for table '{}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> {
            final XmlWriteParser.XmlWrite parsedEvent = XmlWriteParser.parse(row.getRedoSql());
            return new XmlWriteEvent(row, parsedEvent.data(), parsedEvent.length());
        });
    }

    private void handleXmlEndEvent(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, XML_END skipped.");
            return;
        }

        LOGGER.trace("XML_END: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("XM_END for table ' {}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> new XmlEndEvent(row));
    }

    private void handleDataChangeEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (row.getRedoSql() == null) {
            return;
        }

        LOGGER.debug("DML: {}", row);
        LOGGER.trace("\t{}", row.getRedoSql());

        // Oracle LogMiner reports LONG data types as STATUS=2 on UPDATE statements but there is no
        // value in the INFO column, and the record can be managed by the connector successfully,
        // so to be backward compatible, we only explicitly trigger this behavior if there is an
        // error reason for STATUS=2 in the INFO column as well as STATUS=2.
        if (row.getStatus() == 2 && !Strings.isNullOrBlank(row.getInfo())) {
            if (!isUsingHybridStrategy() || (isUsingHybridStrategy() && !isTableKnown(row))) {
                // The SQL in the SQL_REDO column is not valid and cannot be parsed.
                switch (getConfig().getEventProcessingFailureHandlingMode()) {
                    case FAIL:
                        LOGGER.error("Oracle LogMiner is unable to re-construct the SQL for '{}'", row);
                        throw new DebeziumException("Oracle failed to re-construct redo SQL '" + row.getRedoSql() + "'");
                    case WARN:
                        LOGGER.warn("Oracle LogMiner event '{}' cannot be parsed. This event will be ignored and skipped.", row);
                        return;
                    default:
                        // In this case, we explicitly log the situation in "debug" only and not as an error/warn.
                        LOGGER.debug("Oracle LogMiner event '{}' cannot be parsed. This event will be ignored and skipped.", row);
                        return;
                }
            }
        }

        getBatchMetrics().dataChangeEventObserved(row.getEventType());

        final Table table = getTableForDataEvent(row);
        if (table == null) {
            return;
        }

        if (row.isRollbackFlag()) {
            // There is a use case where a constraint violation will result in a DML event being
            // written to the redo log subsequently followed by another DML event that is marked
            // with a rollback flag to indicate that the prior event should be omitted. In this
            // use case, the transaction can still be committed, so we need to manually rollback
            // the previous DML event when this use case occurs.
            removeEventWithRowId(row);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> {
            final LogMinerDmlEntry dmlEntry = parseDmlStatement(row, table);
            dmlEntry.setObjectName(row.getTableName());
            dmlEntry.setObjectOwner(row.getTablespaceName());
            if (getConfig().isLogMiningIncludeRedoSql()) {
                return new RedoSqlDmlEvent(row, dmlEntry, row.getRedoSql());
            }
            return new DmlEvent(row, dmlEntry);
        });

        getMetrics().incrementTotalChangesCount();
    }

    private void handleReplicationMarkerEvent(LogMinerEventRow row) {
        // GoldenGate creates replication markers in the redo logs periodically and these entries can lead to
        // the construction of a transaction in the buffer that never has a COMMIT or ROLLBACK. When this is
        // done by Oracle, we should automatically discard the transaction from the buffer to avoid the low
        // watermark from advancing safely.
        final String transactionId = row.getTransactionId();
        final Transaction transaction = getTransactionCache().getTransaction(transactionId);
        if (transaction != null) {
            LOGGER.debug("Skipping GoldenGate replication marker for transaction {} with SCN {}", transactionId, row.getScn());
            getTransactionCache().removeTransactionEvents(transaction);
            getTransactionCache().removeTransaction(transaction);
        }
        // It should not exist in this cache, but in case.
        getTransactionCache().removeAbandonedTransaction(transactionId);
    }

    private void handleUnsupportedEvent(LogMinerEventRow row) {
        if (!Strings.isNullOrEmpty(row.getTableName())) {
            LOGGER.warn("An unsupported operation detected for table '{}' in transaction {} with SCN {} on redo thread {}.",
                    row.getTableId(),
                    row.getTransactionId(),
                    row.getScn(),
                    row.getThread());
            LOGGER.debug("\t{}", row);
        }
    }

    private Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException {
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
            purgeCache(minCacheScn);
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
        else if (isTransactionIdWithNoSequence(row.getTransactionId())) {
            // This means that Oracle LogMiner found an event that should be undone but its corresponding
            // undo entry was read in a prior mining session and the transaction's sequence could not be
            // resolved.
            final String prefix = getTransactionIdPrefix(row.getTransactionId());
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

    private void handleCommitNotFoundInBuffer(LogMinerEventRow row) {
        // In the event the transaction was prematurely removed due to retention policy, when we do find
        // the transaction's commit in the logs in the future, we should remove the entry if it exists
        // to avoid any potential memory-leak with the cache.
        getTransactionCache().removeAbandonedTransaction(row.getTransactionId());
    }

    private void handleRollbackNotFoundInBuffer(LogMinerEventRow row) {
        // In the event the transaction was prematurely removed due to retention policy, when we do find
        // the transaction's rollback in the logs in the future, we should remove the entry if it exists
        // to avoid any potential memory-leak with the cache.
        getTransactionCache().removeAbandonedTransaction(row.getTransactionId());
    }

    private void purgeCache(Scn minCacheScn) {
        getProcessedTransactionsCache().removeIf(entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
        getSchemaChangesCache().removeIf(entry -> Scn.valueOf(entry.getKey()).compareTo(minCacheScn) < 0);
    }

    private void cleanupAfterTransactionRemovedFromCache(Transaction transaction, boolean isAbandoned) {
        if (isAbandoned) {
            getTransactionCache().abandon(transaction);
        }
        else {
            getTransactionCache().removeAbandonedTransaction(transaction.getTransactionId());
        }
        getTransactionCache().removeTransactionEvents(transaction);
    }

    private void finalizeTransactionCommit(String transactionId, Scn commitScn) {
        getTransactionCache().removeAbandonedTransaction(transactionId);
        // cache recently committed transactions by transaction id
        if (getConfig().isLobEnabled()) {
            getProcessedTransactionsCache().put(transactionId, commitScn.toString());
        }
    }

    private boolean isTransactionSkipped(Transaction transaction) {
        if (transaction != null) {
            // Check whether transaction should be skipped by LogMiner USERNAME field
            if (!Strings.isNullOrBlank(transaction.getUserName())) {
                if (getConfig().getLogMiningUsernameExcludes().contains(transaction.getUserName())) {
                    LOGGER.debug("Skipped transaction with excluded username {}", transaction.getUserName());
                    return true;
                }
            }

            // Check whether transaction should be skipped by LogMiner CLIENT_ID field
            if (!Strings.isNullOrBlank(transaction.getClientId())) {
                if (getConfig().getLogMiningClientIdExcludes().contains(transaction.getClientId())) {
                    LOGGER.debug("Skipped transaction with excluded client id {}", transaction.getClientId());
                    return true;
                }
                else if (!getConfig().getLogMiningClientIdIncludes().isEmpty()) {
                    if (!getConfig().getLogMiningClientIdIncludes().contains(transaction.getClientId())) {
                        LOGGER.debug("Skipped transaction with client id {}", transaction.getClientId());
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void finalizeTransactionRollback(String transactionId, Scn rollbackScn) {
        final Transaction transaction = getTransactionCache().getTransaction(transactionId);
        if (transaction != null) {
            getTransactionCache().removeTransactionEvents(transaction);
            getTransactionCache().removeTransaction(transaction);
        }
        getTransactionCache().removeAbandonedTransaction(transactionId);
        if (getConfig().isLobEnabled()) {
            getProcessedTransactionsCache().put(transactionId, rollbackScn.toString());
        }
    }

    private boolean canAdvanceLowerScnBoundary(LogMinerEventRow row) {
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

    private void warnPotentiallyStuckScn(Scn previousOffsetScn, Map<Integer, Scn> previousOffsetCommitScns) {
        if (getOffsetContext() != null && getOffsetContext().getCommitScn() != null) {
            final Scn scn = getOffsetContext().getScn();
            final Map<Integer, Scn> commitScns = getOffsetContext().getCommitScn().getCommitScnForAllRedoThreads();
            if (previousOffsetScn.equals(scn) && !previousOffsetCommitScns.equals(commitScns)) {
                stuckCount++;
                if (stuckCount == 25) {
                    LOGGER.warn("Offset SCN {} has not changed in 25 mining session iterations. " +
                            "This indicates long running transaction(s) are active.  Commit SCNs {}.",
                            previousOffsetScn,
                            previousOffsetCommitScns);
                    getMetrics().incrementScnFreezeCount();
                    stuckCount = 0;
                }
            }
            else {
                getMetrics().setScnFreezeCount(0);
                stuckCount = 0;
            }
        }
    }

    private Table getTableForDataEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        final TableId tableId = getTableIdForDataEvent(row);
        Table table = getSchema().tableFor(tableId);
        if (table == null) {
            if (!getConfig().getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                return null;
            }
            table = dispatchSchemaChangeEventAndGetTableForNewCapturedTable(tableId);
        }
        return table;
    }

    private TableId getTableIdForDataEvent(LogMinerEventRow row) throws SQLException {
        TableId tableId = row.getTableId();
        if (isUsingHybridStrategy()) {
            if (tableId.table().startsWith("BIN$")) {
                // Object was dropped but has not been purged.
                try (OracleConnection connection = new OracleConnection(getConfig().getJdbcConfig())) {
                    return connection.prepareQueryAndMap("SELECT OWNER, ORIGINAL_NAME FROM DBA_RECYCLEBIN WHERE OBJECT_NAME=?",
                            ps -> ps.setString(1, row.getTableId().table()),
                            rs -> {
                                if (rs.next()) {
                                    return new TableId(row.getTableId().catalog(), rs.getString(1), rs.getString(2));
                                }
                                return row.getTableId();
                            });
                }
            }
            else if (tableId.table().equalsIgnoreCase("UNKNOWN")) {
                // Object has been dropped and purged.
                final TableId resolvedTableId = getSchema().getTableIdByObjectId(row.getObjectId(), row.getDataObjectId());
                if (resolvedTableId != null) {
                    return resolvedTableId;
                }
                throw new DebeziumException("Failed to resolve UNKNOWN table name by object id lookup");
            }
        }
        return tableId;
    }

    private boolean isTableKnown(LogMinerEventRow row) {
        return !row.getTableId().table().equalsIgnoreCase("UNKNOWN");
    }

    private boolean isTableLookupByObjectIdRequired(LogMinerEventRow row) {
        final String tableName = row.getTableId().table();
        if (tableName.startsWith("OBJ# ")) {
            // This is a table that has been dropped and purged
            return true;
        }
        else if (tableName.startsWith("BIN$") && tableName.endsWith("==$0") && tableName.length() == 30) {
            // This is a table that has been dropped, but not yet purged from the RECYCLEBIN
            return true;
        }
        return false;
    }

    private boolean hasNextWithMetricsUpdate(ResultSet resultSet) throws SQLException {
        Instant start = Instant.now();
        boolean result = false;
        try {
            if (resultSet.next()) {
                getMetrics().setLastResultSetNextDuration(Duration.between(start, Instant.now()));
                result = true;
            }

            // Reset sequence unavailability on successful read from the result set
            if (sequenceUnavailable) {
                LOGGER.debug("The previous batch's unavailable log problem has been cleared.");
                sequenceUnavailable = false;
            }
        }
        catch (SQLException e) {
            // Oracle's online redo logs can be defined with dynamic names using the instance
            // configuration property LOG_ARCHIVE_FORMAT.
            //
            // Dynamically named online redo logs can lead to ORA-00310 errors if a log switch
            // happens while the processor is iterating the LogMiner session's result set and
            // LogMiner can no longer read the next batch of records from the log.
            //
            // LogMiner only validates that there are no gaps and that the logs are available
            // when the session is first started and any change in the logs later will raise
            // these types of errors.
            //
            // Catching the ORA-00310 and treating it as the end of the result set will allow
            // the connector's outer loop to re-evaluate the log state and start a new LogMiner
            // session with the new logs. The connector will then begin streaming from where
            // it left off. If any other exception is caught here, it'll be thrown.
            if (!e.getMessage().startsWith("ORA-00310")) {
                // throw any non ORA-00310 error, old behavior
                throw e;
            }
            else if (sequenceUnavailable) {
                // If an ORA-00310 error was raised on the previous iteration and wasn't cleared
                // after re-evaluation of the log availability and the mining session, we will
                // explicitly stop the connector to avoid an infinite loop.
                LOGGER.error("The log availability error '{}' wasn't cleared, stop requested.", e.getMessage());
                throw e;
            }

            LOGGER.debug("A mined log is no longer available: {}", e.getMessage());
            LOGGER.warn("Restarting mining session after a log became unavailable.");

            // Track that we gracefully stopped due to a ORA-00310.
            // Will be used to detect an infinite loop of this error across sequential iterations
            sequenceUnavailable = true;
        }
        return result;
    }

    private void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier) {
        if (getTransactionCache().isAbandoned(transactionId)) {
            LOGGER.warn("Event for abandoned transaction {}, skipped.", transactionId);
            return;
        }
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.warn("Event for transaction {} skipped as transaction has been processed.", transactionId);
            return;
        }

        Transaction transaction = getTransactionCache().getTransaction(transactionId);
        if (transaction != null && isTransactionOverEventThreshold(transaction)) {
            abandonTransactionOverEventThreshold(transaction);
            return;
        }

        final LogMinerEvent event;
        try {
            event = eventSupplier.get();
        }
        catch (DmlParserException e) {
            switch (getConfig().getEventProcessingFailureHandlingMode()) {
                case FAIL:
                    Loggings.logErrorAndTraceRecord(LOGGER, row, "Failed to parse SQL for event");
                    throw e;
                case WARN:
                    Loggings.logWarningAndTraceRecord(LOGGER, row, "Failed to parse redo SQL, event is being ignored and skipped.");
                    return;
                default:
                    // In this case, we explicitly log the situation in "debug" only and not as an error/warn.
                    Loggings.logDebugAndTraceRecord(LOGGER, row, "Failed to parse redo SQL, event is being ignored and skipped.");
                    return;
            }
        }

        if (transaction == null) {
            LOGGER.trace("Transaction {} is not in cache, creating.", transactionId);
            transaction = transactionFactory.createTransaction(row);
            getTransactionCache().addTransaction(transaction);
        }

        final int eventId = transaction.getNextEventId();
        if (!getTransactionCache().containsTransactionEvent(transaction, eventId)) {
            // Add new event at eventId offset
            LOGGER.trace("Transaction {}, adding event reference at key {}", transactionId, transaction.getEventId(eventId));
            getTransactionCache().addTransactionEvent(transaction, eventId, event);
            getMetrics().calculateLagFromSource(row.getChangeTime());
        }

        // When using Infinispan, this extra put is required so that the state is properly synchronized
        getTransactionCache().syncTransaction(transaction);
        getMetrics().setActiveTransactionCount(getTransactionCache().getTransactionCount());
    }

    @VisibleForTesting
    protected Table dispatchSchemaChangeEventAndGetTableForNewCapturedTable(TableId tableId)
            throws SQLException, InterruptedException {

        LOGGER.warn("Obtaining schema for table {}, which should be already loaded, this may signal potential bug in fetching table schemas.", tableId);
        try (OracleConnection connection = new OracleConnection(getConfig().getJdbcConfig(), false)) {
            connection.setAutoCommit(false);
            if (!Strings.isNullOrBlank(getConfig().getPdbName())) {
                connection.setSessionToPdb(getConfig().getPdbName());
            }

            final String tableDdl = getTableMetadataDdl(connection, tableId);
            final Long objectId = connection.getTableObjectId(tableId);
            final Long dataObjectId = connection.getTableDataObjectId(tableId);

            getEventDispatcher().dispatchSchemaChangeEvent(
                    getPartition(),
                    getOffsetContext(),
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            getConfig(),
                            getPartition(),
                            getOffsetContext(),
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            objectId,
                            dataObjectId,
                            tableDdl,
                            getSchema(),
                            Instant.now(),
                            getMetrics(),
                            null));

            return getSchema().tableFor(tableId);
        }
        catch (OracleConnection.NonRelationalTableException e) {
            LOGGER.warn("Table {} is not a relational table and will be skipped.", tableId);
            getMetrics().incrementWarningCount();
            return null;
        }
    }

    @VisibleForTesting
    protected String getTableMetadataDdl(OracleConnection connection, TableId tableId) throws SQLException, OracleConnection.NonRelationalTableException {
        getBatchMetrics().tableMetadataQueryObserved();
        LOGGER.info("Getting database metadata for table '{}'", tableId);
        return connection.getTableMetadataDdl(tableId);
    }

    private LogMinerDmlEntry parseDmlStatement(LogMinerEventRow row, Table table) {
        LogMinerDmlEntry dmlEntry;
        final String redoSql = row.getRedoSql();
        try {
            Instant parseStart = Instant.now();
            dmlEntry = resolveParser(row).parse(redoSql, table);
            getMetrics().setLastParseTimeDuration(Duration.between(parseStart, Instant.now()));
        }
        catch (DmlParserException e) {
            String message = "DML statement couldn't be parsed." +
                    " Please open a Jira issue with the statement '" + redoSql + "'.";
            throw new DmlParserException(message, e);
        }

        if (dmlEntry.getOldValues().length == 0) {
            if (EventType.UPDATE == dmlEntry.getEventType() || EventType.DELETE == dmlEntry.getEventType()) {
                LOGGER.warn("The DML event '{}' contained no before state.", redoSql);
                getMetrics().incrementWarningCount();
            }
        }

        return dmlEntry;
    }

    private LogMinerDmlParser resolveParser(LogMinerEventRow row) {
        if (row.getStatus() == 2 && !Strings.isNullOrBlank(row.getInfo()) && isUsingHybridStrategy()) {
            return reconstructColumnDmlParser;
        }
        return dmlParser;
    }

    private boolean isUsingHybridStrategy() {
        return OracleConnectorConfig.LogMiningStrategy.HYBRID.equals(getConfig().getLogMiningStrategy());
    }

    private String parseExtendedStringWriteSql(String sql) {
        int endIndex = sql.lastIndexOf(";");
        if (endIndex == -1) {
            throw new DebeziumException("Failed to find end index on 32K_WRITE operation");
        }

        endIndex = sql.lastIndexOf(";", endIndex - 1);
        if (endIndex == -1) {
            throw new DebeziumException("Failed to find end index on 32K_WRITE operation");
        }

        return sql.substring(12, endIndex - 1);
    }

    private boolean isTransactionIdWithNoSequence(String transactionId) {
        return transactionId.endsWith(NO_SEQUENCE_TRX_ID_SUFFIX);
    }

    private String getTransactionIdPrefix(String transactionId) {
        return transactionId.substring(0, 8);
    }

    private boolean isTransactionOverEventThreshold(Transaction transaction) {
        if (getConfig().getLogMiningBufferTransactionEventsThreshold() == 0) {
            return false;
        }
        return getTransactionEventCount(transaction) >= getConfig().getLogMiningBufferTransactionEventsThreshold();
    }

    private void abandonTransactionOverEventThreshold(Transaction transaction) {
        LOGGER.warn("Transaction {} exceeds maximum allowed number of events, transaction will be abandoned.", transaction.getTransactionId());
        getMetrics().incrementWarningCount();
        getTransactionCache().getAndRemoveTransaction(transaction.getTransactionId());
        cleanupAfterTransactionRemovedFromCache(transaction, true);
        getMetrics().incrementOversizedTransactionCount();
    }

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
            LOGGER.error(String.format("Cannot fetch SCN %s by given duration to calculate SCN to abandon", lastProcessedScn), e);
            getMetrics().incrementErrorCount();
            return Optional.empty();
        }
    }

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

    private boolean isNonIncludedTableSkipped(LogMinerEventRow row) {
        if (isUsingHybridStrategy()) {
            if (isTableLookupByObjectIdRequired(row)) {
                // Special use case where the table has been dropped and purged, and we are processing an
                // old event for the table that comes prior to the drop.
                LOGGER.trace("Found DML for dropped table in history with object-id based table name {}.", row.getTableId().table());
                final TableId tableId = getSchema().getTableIdByObjectId(row.getObjectId(), null);
                if (tableId != null) {
                    row.setTableId(tableId);
                }
                return !tableFilter.isIncluded(row.getTableId());
            }
        }
        return true;
    }
}
