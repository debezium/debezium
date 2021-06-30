/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.BlobChunkList;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Buffer that stores transactions and related callbacks that will be executed when a transaction commits or discarded
 * when a transaction has been rolled back.
 *
 * @author Andrey Pustovetov
 */
@NotThreadSafe
public final class TransactionalBuffer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private final OracleConnectorConfig connectorConfig;
    private final Map<String, Transaction> transactions;
    private final OracleDatabaseSchema schema;
    private final Clock clock;
    private final ErrorHandler errorHandler;
    private final Set<String> abandonedTransactionIds;
    private final Set<String> rolledBackTransactionIds;
    private final Set<RecentlyCommittedTransaction> recentlyCommittedTransactionIds;
    private final Set<Scn> recentlyEmittedDdls;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    private Scn lastCommittedScn;
    private Scn maxCommittedScn;

    /**
     * Constructor to create a new instance.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @param schema database schema
     * @param clock system clock
     * @param errorHandler the connector error handler
     * @param streamingMetrics the streaming metrics
     */
    TransactionalBuffer(OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema, Clock clock, ErrorHandler errorHandler,
                        OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.transactions = new HashMap<>();
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.clock = clock;
        this.errorHandler = errorHandler;
        this.lastCommittedScn = Scn.NULL;
        this.maxCommittedScn = Scn.NULL;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();
        this.recentlyCommittedTransactionIds = new HashSet<>();
        this.recentlyEmittedDdls = new HashSet<>();
        this.streamingMetrics = streamingMetrics;
    }

    /**
     * @return rolled back transactions
     */
    Set<String> getRolledBackTransactionIds() {
        return new HashSet<>(rolledBackTransactionIds);
    }

    /**
     * Registers a DDL operation with the buffer.
     *
     * @param scn the system change number
     */
    void registerDdlOperation(Scn scn) {
        recentlyEmittedDdls.add(scn);
    }

    /**
     * Returns whether the ddl operation has been registered.
     *
     * @param scn the system change number
     * @return true if the ddl operation has been seen and processed, false otherwise.
     */
    boolean isDdlOperationRegistered(Scn scn) {
        return recentlyEmittedDdls.contains(scn);
    }

    /**
     * Register a DML operation with the transaction buffer.
     *
     * @param operation operation type
     * @param transactionId unique transaction identifier
     * @param scn system change number
     * @param tableId table identifier
     * @param parseEntry parser entry
     * @param changeTime time the DML operation occurred
     * @param rowId unique row identifier
     * @param rsId rollback sequence identifier
     * @param hash unique row hash
     */
    void registerDmlOperation(int operation, String transactionId, Scn scn, TableId tableId, LogMinerDmlEntry parseEntry,
                              Instant changeTime, String rowId, Object rsId, long hash) {
        if (registerEvent(transactionId, scn, hash, changeTime, () -> new DmlEvent(operation, parseEntry, scn, tableId, rowId, rsId))) {
            streamingMetrics.incrementRegisteredDmlCount();
        }
    }

    /**
     * Register a {@code SEL_LOB_LOCATOR} operation with the transaction buffer.
     *
     * @param operation operation type
     * @param transactionId unique transaction identifier
     * @param scn system change number
     * @param tableId table identifier
     * @param parseEntry parser entry
     * @param changeTime time the operation occurred
     * @param rowId unique row identifier
     * @param rsId rollback sequence identifier
     * @param hash unique row hash
     */
    void registerSelectLobOperation(int operation, String transactionId, Scn scn, TableId tableId, LogMinerDmlEntry parseEntry,
                                    String columnName, boolean binaryData, Instant changeTime, String rowId, Object rsId, long hash) {
        registerEvent(transactionId, scn, hash, changeTime,
                () -> new SelectLobLocatorEvent(operation, parseEntry, columnName, binaryData, scn, tableId, rowId, rsId));
    }

    /**
     * Register a {@code LOB_WRITE} operation with the transaction buffer.
     *
     * @param operation operation type
     * @param transactionId unique transaction identifier
     * @param scn system change number
     * @param tableId table identifier
     * @param data data written by the LOB operation
     * @param changeTime time the operation occurred
     * @param rowId unique row identifier
     * @param rsId rollback sequence identifier
     * @param hash unique row hash
     */
    void registerLobWriteOperation(int operation, String transactionId, Scn scn, TableId tableId, String data,
                                   Instant changeTime, String rowId, Object rsId, long hash) {
        if (data != null) {
            final String sql = parseLobWriteSql(data);
            registerEvent(transactionId, scn, hash, changeTime,
                    () -> new LobWriteEvent(operation, sql, scn, tableId, rowId, rsId));

        }
    }

    /**
     * Register a {@code LOB_ERASE} operation with the transction buffer.
     *
     * @param operation operation type
     * @param transactionId unique transaction identifier
     * @param scn system change number
     * @param tableId table identifier
     * @param changeTime time the operation occurred
     * @param rowId unique row identifier
     * @param rsId rollback sequence identifier
     * @param hash unique row hash
     */
    void registerLobEraseOperation(int operation, String transactionId, Scn scn, TableId tableId, Instant changeTime,
                                   String rowId, Object rsId, long hash) {
        registerEvent(transactionId, scn, hash, changeTime, () -> new LobEraseEvent(operation, scn, tableId, rowId, rsId));
    }

    /**
     * Undo a staged DML operation in the transaction buffer.
     *
     * @param transactionId unique transaction identifier
     * @param undoRowId unique row identifier to be undone
     * @param tableId table identifier
     */
    void undoDmlOperation(String transactionId, String undoRowId, TableId tableId) {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            LOGGER.warn("Cannot undo changes to {} with row id {} as transaction {} not found.", tableId, undoRowId, transactionId);
            return;
        }

        transaction.events.removeIf(o -> {
            if (o.getRowId().equals(undoRowId)) {
                LOGGER.trace("Undoing change to {} with row id {} in transaction {}", tableId, undoRowId, transactionId);
                return true;
            }
            return false;
        });
    }

    /**
     * Register a new transaction with the transaction buffer.
     *
     * @param transactionId unique transaction identifier
     * @param scn starting SCN of the transaction
     */
    void registerTransaction(String transactionId, Scn scn) {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null && !isRecentlyCommitted(transactionId)) {
            transactions.put(transactionId, new Transaction(transactionId, scn));
            streamingMetrics.setActiveTransactions(transactions.size());
        }
    }

    /**
     * Commits a transaction by looking up the transaction in the buffer and if exists, all registered callbacks
     * will be executed in chronological order, emitting events for each followed by a transaction commit event.
     *
     * @param transactionId transaction identifier
     * @param scn           SCN of the commit.
     * @param offsetContext Oracle offset
     * @param timestamp     commit timestamp
     * @param context       context to check that source is running
     * @param debugMessage  message
     * @param dispatcher    event dispatcher
     * @return true if committed transaction is in the buffer, was not processed yet and processed now
     */
    boolean commit(String transactionId, Scn scn, OracleOffsetContext offsetContext, Timestamp timestamp,
                   ChangeEventSource.ChangeEventSourceContext context, String debugMessage, EventDispatcher<TableId> dispatcher) {

        Instant start = Instant.now();
        Transaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            return false;
        }

        Scn smallestScn = calculateSmallestScn();

        abandonedTransactionIds.remove(transactionId);

        if (isRecentlyCommitted(transactionId)) {
            return false;
        }

        // On the restarting connector, we start from SCN in the offset. There is possibility to commit a transaction(s) which were already committed.
        // Currently we cannot use ">=", because we may lose normal commit which may happen at the same time. TODO use audit table to prevent duplications
        if ((offsetContext.getCommitScn() != null && offsetContext.getCommitScn().compareTo(scn) > 0) || lastCommittedScn.compareTo(scn) > 0) {
            LOGGER.debug("Transaction {} already processed, ignored. Committed SCN in offset is {}, commit SCN of the transaction is {}, last committed SCN is {}",
                    transactionId, offsetContext.getCommitScn(), scn, lastCommittedScn);
            streamingMetrics.setActiveTransactions(transactions.size());
            return false;
        }

        reconcileTransaction(transaction);

        LOGGER.trace("COMMIT, {}, smallest SCN: {}", debugMessage, smallestScn);
        try {
            int counter = transaction.events.size();
            for (LogMinerEvent event : transaction.events) {
                if (!context.isRunning()) {
                    return false;
                }

                // Update SCN in offset context only if processed SCN less than SCN among other transactions
                if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                    offsetContext.setScn(event.getScn());
                    streamingMetrics.setOldestScn(event.getScn());
                }

                offsetContext.setTransactionId(transaction.transactionId);
                offsetContext.setSourceTime(timestamp.toInstant());
                offsetContext.setTableId(event.getTableId());
                if (--counter == 0) {
                    offsetContext.setCommitScn(scn);
                }

                LOGGER.trace("Processing event {}", event);
                dispatcher.dispatchDataChangeEvent(event.getTableId(),
                        new LogMinerChangeRecordEmitter(
                                offsetContext,
                                event.getOperation(),
                                event.getEntry().getOldValues(),
                                event.getEntry().getNewValues(),
                                schema.tableFor(event.getTableId()),
                                clock));

            }

            lastCommittedScn = Scn.valueOf(scn.longValue());
            if (!transaction.events.isEmpty()) {
                dispatcher.dispatchTransactionCommittedEvent(offsetContext);
            }
            else {
                dispatcher.dispatchHeartbeatEvent(offsetContext);
            }

            streamingMetrics.calculateLagMetrics(timestamp.toInstant());

            if (lastCommittedScn.compareTo(maxCommittedScn) > 0) {
                LOGGER.trace("Updated transaction buffer max commit SCN to '{}'", lastCommittedScn);
                maxCommittedScn = lastCommittedScn;
            }

            if (connectorConfig.isLobEnabled()) {
                // cache recent transaction and commit scn for handling offset updates
                recentlyCommittedTransactionIds.add(new RecentlyCommittedTransaction(transaction, scn));
            }
        }
        catch (InterruptedException e) {
            LogMinerHelper.logError(streamingMetrics, "Commit interrupted", e);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            streamingMetrics.incrementCommittedTransactions();
            streamingMetrics.setActiveTransactions(transactions.size());
            streamingMetrics.incrementCommittedDmlCount(transaction.events.size());
            streamingMetrics.setCommittedScn(scn);
            streamingMetrics.setOffsetScn(offsetContext.getScn());
            streamingMetrics.setLastCommitDuration(Duration.between(start, Instant.now()));
        }

        return true;
    }

    /**
     * Update the offset context based on the current state of the transaction buffer.
     *
     * @param offsetContext offset context, should not be {@code null}
     * @param dispatcher event dispatcher, should not be {@code null}
     * @return offset context SCN, never {@code null}
     * @throws InterruptedException thrown if dispatch of heartbeat event fails
     */
    Scn updateOffsetContext(OracleOffsetContext offsetContext, EventDispatcher<TableId> dispatcher) throws InterruptedException {
        if (transactions.isEmpty()) {
            if (!maxCommittedScn.isNull()) {
                LOGGER.trace("Transaction buffer is empty, updating offset SCN to '{}'", maxCommittedScn);
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(offsetContext);
            }
            else {
                LOGGER.trace("No max committed SCN detected, offset SCN still '{}'", offsetContext.getScn());
            }
        }
        else {
            Scn minStartScn = transactions.values().stream().map(t -> t.firstScn).min(Scn::compareTo).orElse(Scn.NULL);
            if (!minStartScn.isNull()) {
                LOGGER.trace("Removing all commits up to SCN '{}'", minStartScn);
                recentlyCommittedTransactionIds.removeIf(t -> t.firstScn.compareTo(minStartScn) < 0);
                LOGGER.trace("Removing all tracked DDL operations up to SCN '{}'", minStartScn);
                recentlyEmittedDdls.removeIf(scn -> scn.compareTo(minStartScn) < 0);
                offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                dispatcher.dispatchHeartbeatEvent(offsetContext);
            }
            else {
                LOGGER.trace("Minimum SCN in transaction buffer is still SCN '{}'", minStartScn);
            }
        }
        return offsetContext.getScn();
    }

    /**
     * Clears registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction id
     * @param debugMessage  message
     * @return true if the rollback is for a transaction in the buffer
     */
    boolean rollback(String transactionId, String debugMessage) {

        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            LOGGER.debug("Transaction rolled back: {}", debugMessage);

            transactions.remove(transactionId);
            abandonedTransactionIds.remove(transactionId);
            rolledBackTransactionIds.add(transactionId);

            streamingMetrics.setActiveTransactions(transactions.size());
            streamingMetrics.incrementRolledBackTransactions();
            streamingMetrics.addRolledBackTransactionId(transactionId);

            return true;
        }

        return false;
    }

    /**
     * If for some reason the connector got restarted, the offset will point to the beginning of the oldest captured transaction.
     * If that transaction was lasted for a long time, let say > 4 hours, the offset might be not accessible after restart,
     * Hence we have to address these cases manually.
     * <p>
     * In case of an abandonment, all DMLs/Commits/Rollbacks for this transaction will be ignored
     *
     * @param thresholdScn the smallest SVN of any transaction to keep in the buffer. All others will be removed.
     * @param offsetContext the offset context
     */
    void abandonLongTransactions(Scn thresholdScn, OracleOffsetContext offsetContext) {
        LogMinerHelper.logWarn(streamingMetrics, "All transactions with first SCN <= {} will be abandoned, offset: {}", thresholdScn, offsetContext.getScn());
        Scn threshold = Scn.valueOf(thresholdScn.toString());
        Scn smallestScn = calculateSmallestScn();
        if (smallestScn == null) {
            // no transactions in the buffer
            return;
        }
        if (threshold.compareTo(smallestScn) < 0) {
            threshold = smallestScn;
        }
        Iterator<Map.Entry<String, Transaction>> iter = transactions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Transaction> transaction = iter.next();
            if (transaction.getValue().firstScn.compareTo(threshold) <= 0) {
                LogMinerHelper.logWarn(streamingMetrics, "Following long running transaction {} will be abandoned and ignored: {} ", transaction.getKey(),
                        transaction.getValue().toString());
                abandonedTransactionIds.add(transaction.getKey());
                iter.remove();

                streamingMetrics.addAbandonedTransactionId(transaction.getKey());
                streamingMetrics.setActiveTransactions(transactions.size());
            }
        }
    }

    boolean isTransactionRegistered(String txId) {
        return transactions.get(txId) != null;
    }

    private Scn calculateSmallestScn() {
        Scn scn = transactions.isEmpty() ? null
                : transactions.values()
                        .stream()
                        .map(transaction -> transaction.firstScn)
                        .min(Scn::compareTo)
                        .orElseThrow(() -> new DataException("Cannot calculate smallest SCN"));
        streamingMetrics.setOldestScn(scn == null ? Scn.valueOf(-1) : scn);
        return scn;
    }

    /**
     * Returns {@code true} if buffer is empty, otherwise {@code false}.
     *
     * @return {@code true} if buffer is empty, otherwise {@code false}
     */
    boolean isEmpty() {
        return transactions.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        this.transactions.values().forEach(t -> result.append(t.toString()));
        return result.toString();
    }

    @Override
    public void close() {
        transactions.clear();
    }

    /**
     * Helper method to register a given {@link LogMinerEvent} implementation with the buffer.
     * If the event is registered, the underlying metrics active transactions and lag will be re-calculated.
     *
     * @param transactionId transaction id that contained the given event
     * @param scn system change number for the event
     * @param hash unique hash that identifies the row in a transaction
     * @param changeTime the time the event occurred
     * @param supplier supplier function to generate the event if validity checks pass
     * @return true if the event was registered, false otherwise
     */
    private boolean registerEvent(String transactionId, Scn scn, long hash, Instant changeTime, Supplier<LogMinerEvent> supplier) {
        if (abandonedTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(streamingMetrics, "Event for abandoned transaction {}, ignored.", transactionId);
            return false;
        }
        if (rolledBackTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(streamingMetrics, "Event for rolled back transaction {}, ignored.", transactionId);
            return false;
        }
        if (isRecentlyCommitted(transactionId)) {
            LOGGER.trace("Event for transaction {} skipped, transaction already committed.", transactionId);
            return false;
        }

        Transaction transaction = transactions.computeIfAbsent(transactionId, s -> new Transaction(transactionId, scn));

        // Event will only be registered with transaction if the computed hash doesn't already exist.
        // This is necessary to handle overlapping mining session scopes
        if (!transaction.eventHashes.contains(hash)) {
            transaction.eventHashes.add(hash);
            transaction.events.add(supplier.get());

            streamingMetrics.setActiveTransactions(transactions.size());
            streamingMetrics.calculateLagMetrics(changeTime);
            return true;
        }
        return false;
    }

    /**
     * Returns whether the specified transaction has recently been committed.
     *
     * @param transactionId the transaction identifier
     * @return true if the transaction has been recently committed (seen by the connector), otherwise false.
     */
    private boolean isRecentlyCommitted(String transactionId) {
        if (recentlyCommittedTransactionIds.isEmpty()) {
            return false;
        }

        for (RecentlyCommittedTransaction transaction : recentlyCommittedTransactionIds) {
            if (transaction.transactionId.equals(transactionId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parses a {@code LOB_WRITE} operation SQL fragment.
     *
     * @param sql sql statement
     * @return the parsed statement
     * @throws DebeziumException if an unexpected SQL fragment is provided that cannot be parsed
     */
    private String parseLobWriteSql(String sql) {
        if (sql == null) {
            return null;
        }

        int start = sql.indexOf(":= '");
        if (start != -1) {
            // LOB_WRITE SQL is for a CLOB field
            int end = sql.lastIndexOf("'");
            return sql.substring(start + 4, end);
        }

        start = sql.indexOf(":= HEXTORAW");
        if (start != -1) {
            // LOB_WRITE SQL is for a BLOB field
            int end = sql.lastIndexOf("'") + 2;
            return sql.substring(start + 3, end);
        }

        throw new DebeziumException("Unable to parse unsupported LOB_WRITE SQL: " + sql);
    }

    /**
     * Reconcile the specified transaction by merging multiple events that should be emitted as a single
     * logical event, such as changes made to LOB column types that involve multiple events.
     *
     * @param transaction transaction to be reconciled, never {@code null}
     */
    private void reconcileTransaction(Transaction transaction) {
        // Do not perform reconciliation if LOB support is not enabled.
        if (!connectorConfig.isLobEnabled()) {
            return;
        }

        LOGGER.trace("Reconciling transaction {}", transaction.transactionId);
        LogMinerEvent prevEvent = null;

        int prevEventSize = transaction.events.size();
        for (int i = 0; i < transaction.events.size();) {

            final LogMinerEvent event = transaction.events.get(i);
            LOGGER.trace("Processing event {}", event);

            switch (event.getOperation()) {
                case RowMapper.SELECT_LOB_LOCATOR:
                    if (shouldMergeSelectLobLocatorEvent(transaction, i, (SelectLobLocatorEvent) event, prevEvent)) {
                        continue;
                    }
                    break;
                case RowMapper.INSERT:
                case RowMapper.UPDATE:
                    if (shouldMergeDmlEvent(transaction, i, (DmlEvent) event, prevEvent)) {
                        continue;
                    }
                    break;
            }

            ++i;
            prevEvent = event;
            LOGGER.trace("Previous event is now {}", prevEvent);
        }

        if (transaction.events.size() != prevEventSize) {
            LOGGER.trace("Reconciled transaction {} from {} events to {}.", transaction.transactionId, prevEventSize, transaction.events.size());
        }
        else {
            LOGGER.trace("Transaction {} event queue was unmodified.", transaction.transactionId);
        }
    }

    /**
     * Attempts to merge the provided SEL_LOB_LOCATOR event with the previous event in the transaction.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index event index being processed
     * @param event event being processed, never {@code null}
     * @param prevEvent previous event in the transaction, can be {@code null}
     * @return true if the event is merged, false if the event was not merged.
     */
    private boolean shouldMergeSelectLobLocatorEvent(Transaction transaction, int index, SelectLobLocatorEvent event, LogMinerEvent prevEvent) {
        LOGGER.trace("\tDetected SelectLobLocatorEvent for column '{}'", event.getColumnName());

        final int columnIndex = LogMinerHelper.getColumnIndexByName(event.getColumnName(), schema.tableFor(event.getTableId()));

        // Read and combine all LOB_WRITE events that follow SEL_LOB_LOCATOR
        Object lobData = null;
        final List<String> lobWrites = readAndCombineLobWriteEvents(transaction, index, event.isBinaryData());
        if (!lobWrites.isEmpty()) {
            if (event.isBinaryData()) {
                // For BLOB we pass the list of string chunks as-is to the value converter
                lobData = new BlobChunkList(lobWrites);
            }
            else {
                // For CLOB we go ahead and pre-process the List into a single string.
                lobData = String.join("", lobWrites);
            }
        }

        // Read and consume all LOB_ERASE events that follow SEL_LOB_LOCATOR
        final int lobEraseEvents = readAndConsumeLobEraseEvents(transaction, index);
        if (lobEraseEvents > 0) {
            LOGGER.warn("LOB_ERASE for table '{}' column '{}' is not supported, use DML operations to manipulate LOB columns only.", event.getTableId(),
                    event.getColumnName());
            if (lobWrites.isEmpty()) {
                // There are no write and only erase events, discard entire SEL_LOB_LOCATOR
                // To simulate this, we treat this as a "merge" op so caller doesn't modify previous event
                transaction.events.remove(index);
                return true;
            }
        }
        else if (lobEraseEvents == 0 && lobWrites.isEmpty()) {
            // There were no LOB operations present, discard entire SEL_LOB_LOCATOR
            // To simulate this, we treat this as a "merge" op so caller doesn't modify previous event
            transaction.events.remove(index);
            return true;
        }

        // SelectLobLocatorEvent can be treated as a parent DML operation where an update occurs on any
        // LOB-based column. In this case, the event will be treated as an UPDATE event when emitted.

        if (prevEvent == null) {
            // There is no prior event, add column to this SelectLobLocatorEvent and don't merge.
            LOGGER.trace("\tAdding column '{}' to current event", event.getColumnName());
            event.getEntry().getNewValues()[columnIndex] = lobData;
            return false;
        }

        if (RowMapper.INSERT == prevEvent.getOperation()) {
            // Previous event is an INSERT operation.
            // Only merge the SEL_LOB_LOCATOR event if the previous INSERT is for the same table/row
            // and if the INSERT's column value is EMPTY_CLOB() or EMPTY_BLOB()
            if (isForSameTableOrScn(event, prevEvent)) {
                LOGGER.trace("\tMerging SEL_LOB_LOCATOR with previous INSERT event");
                Object prevValue = prevEvent.getEntry().getNewValues()[columnIndex];
                if (!"EMPTY_CLOB()".equals(prevValue) && !"EMPTY_BLOB()".equals(prevValue)) {
                    throw new DebeziumException("Expected to find column '" + event.getColumnName() + "' in table '"
                            + prevEvent.getTableId() + "' to be initialized as an empty LOB value.'");
                }

                prevEvent.getEntry().getNewValues()[columnIndex] = lobData;

                // Remove the SEL_LOB_LOCATOR event from event list and indicate merged.
                transaction.events.remove(index);
                return true;
            }
        }
        else if (RowMapper.UPDATE == prevEvent.getOperation()) {
            // Previous event is an UPDATE operation.
            // Only merge the SEL_LOB_LOCATOR event if the previous UPDATE is for the same table/row
            if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                LOGGER.trace("\tUpdating SEL_LOB_LOCATOR column '{}' to previous UPDATE event", event.getColumnName());
                prevEvent.getEntry().getNewValues()[columnIndex] = lobData;

                // Remove the SEL_LOB_LOCATOR event from event list and indicate merged.
                transaction.events.remove(index);
                return true;
            }
        }
        else if (RowMapper.SELECT_LOB_LOCATOR == prevEvent.getOperation()) {
            // Previous event is a SEL_LOB_LOCATOR operation.
            // Only merge the two SEL_LOB_LOCATOR events if they're for the same table/row
            if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                LOGGER.trace("\tAdding column '{}' to previous SEL_LOB_LOCATOR event", event.getColumnName());
                prevEvent.getEntry().getNewValues()[columnIndex] = lobData;

                // Remove the SEL_LOB_LOCATOR event from event list and indicate merged.
                transaction.events.remove(index);
                return true;
            }
        }
        else {
            throw new DebeziumException("Unexpected previous event operation: " + prevEvent.getOperation());
        }

        LOGGER.trace("\tSEL_LOB_LOCATOR event is for different row, merge skipped.");
        LOGGER.trace("\tAdding column '{}' to current event", event.getColumnName());
        event.getEntry().getNewValues()[columnIndex] = lobData;
        return false;
    }

    /**
     * Attempts to merge the provided DML event with the previous event in the transaction.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index event index being processed
     * @param event event being processed, never {@code null}
     * @param prevEvent previous event in the transaction, can be {@code null}
     * @return true if the event is merged, false if the event was not merged
     */
    private boolean shouldMergeDmlEvent(Transaction transaction, int index, DmlEvent event, LogMinerEvent prevEvent) {
        LOGGER.trace("\tDetected DmlEvent {}", event.getOperation());

        if (prevEvent == null) {
            // There is no prior event, therefore there is no reason to perform any merge.
            return false;
        }

        if (RowMapper.INSERT == prevEvent.getOperation()) {
            // Previous event is an INSERT operation.
            // The only valid combination here would be if the current event is an UPDATE since an INSERT cannot
            // be merged with a prior INSERT with how LogMiner materializes the rows.
            if (RowMapper.UPDATE == event.getOperation()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tMerging UPDATE event with previous INSERT event");
                    mergeNewColumns(event, prevEvent);

                    // Remove the UPDATE event from event list and indicate merged.
                    transaction.events.remove(index);
                    return true;
                }
            }
        }
        else if (RowMapper.UPDATE == prevEvent.getOperation()) {
            // Previous event is an UPDATE operation.
            // This will happen if there are non-CLOB and inline-CLOB fields updated in the same SQL.
            // The inline-CLOB values should be merged with the previous UPDATE event.
            if (RowMapper.UPDATE == event.getOperation()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tMerging UPDATE event with previous UPDATE event");
                    mergeNewColumns(event, prevEvent);

                    // Remove the UPDATE event from event list and indicate merged.
                    transaction.events.remove(index);
                    return true;
                }
            }
        }
        else if (RowMapper.SELECT_LOB_LOCATOR == prevEvent.getOperation()) {
            // Previous event is a SEL_LOB_LOCATOR operation.
            // SQL contained both non-inline CLOB and inline-CLOB field changes.
            if (RowMapper.UPDATE == event.getOperation()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tMerging UPDATE event with previous SEL_LOB_LOCATOR event");
                    for (int i = 0; i < event.getEntry().getNewValues().length; ++i) {
                        Object value = event.getEntry().getNewValues()[i];
                        Object prevValue = prevEvent.getEntry().getNewValues()[i];
                        if (prevValue == null && value != null) {
                            LOGGER.trace("\tAdding column index {} to previous SEL_LOB_LOCATOR event", i);
                            prevEvent.getEntry().getNewValues()[i] = value;
                        }
                    }

                    // Remove the UPDATE event from event list and indicate merged.
                    transaction.events.remove(index);
                    return true;
                }
            }
        }

        LOGGER.trace("\tDmlEvent {} event is for different row, merge skipped.", event.getOperation());
        return false;
    }

    /**
     * Reads the transaction event queue and combines all LOB_WRITE events starting at the provided index.
     * for a SEL_LOB_LOCATOR event which is for binary data (BLOB) data types.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index index to the first LOB_WRITE operation
     * @return list of string-based values for each LOB_WRITE operation
     */
    private List<String> readAndCombineLobWriteEvents(Transaction transaction, int index, boolean binaryData) {
        List<String> chunks = new ArrayList<>();
        for (int i = index + 1; i < transaction.events.size(); ++i) {
            final LogMinerEvent event = transaction.events.get(i);
            if (!(event instanceof LobWriteEvent)) {
                break;
            }

            final LobWriteEvent writeEvent = (LobWriteEvent) event;
            if (binaryData && !writeEvent.getData().startsWith("HEXTORAW('") && !writeEvent.getData().endsWith("')")) {
                throw new DebeziumException("Unexpected BLOB data chunk: " + writeEvent.getData());
            }

            chunks.add(writeEvent.getData());
        }

        if (!chunks.isEmpty()) {
            LOGGER.trace("\tCombined {} LobWriteEvent events", chunks.size());
            // Remove events from the transaction queue queue
            for (int i = 0; i < chunks.size(); ++i) {
                transaction.events.remove(index + 1);
            }
        }

        return chunks;
    }

    /**
     * Read and remove all LobErase events detected in the transaction event queue.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index index to the first LOB_ERASE operation
     * @return number of LOB_ERASE events consumed and removed from the event queue
     */
    private int readAndConsumeLobEraseEvents(Transaction transaction, int index) {
        int events = 0;
        for (int i = index + 1; i < transaction.events.size(); ++i) {
            final LogMinerEvent event = transaction.events.get(i);
            if (!(event instanceof LobEraseEvent)) {
                break;
            }
            events++;
        }

        if (events > 0) {
            LOGGER.trace("\tConsumed {} LobErase events", events);
            for (int i = 0; i < events; ++i) {
                transaction.events.remove(index + 1);
            }
        }

        return events;
    }

    /**
     * Checks whether the two events are for the same table or participate in the same system change.
     *
     * @param event current event being processed, never {@code null}
     * @param prevEvent previous/parent event that has been processed, may be {@code null}
     * @return true if the two events are for the same table or system change number, false otherwise
     */
    private boolean isForSameTableOrScn(LogMinerEvent event, LogMinerEvent prevEvent) {
        if (prevEvent != null) {
            if (event.getTableId().equals(prevEvent.getTableId())) {
                return true;
            }
            return event.getScn().equals(prevEvent.getScn()) && event.getRsId().equals(prevEvent.getRsId());
        }
        return false;
    }

    /**
     * Checks whether the two events are for the same table row.
     *
     * @param event current event being processed, never {@code null}
     * @param prevEvent previous/parent event that has been processed, never {@code null}
     * @return true if the two events are for the same table row, false otherwise
     */
    private boolean isSameTableRow(LogMinerEvent event, LogMinerEvent prevEvent) {
        final Table table = schema.tableFor(event.getTableId());
        if (table == null) {
            LOGGER.trace("Unable to locate table '{}' schema, unable to detect if same row.", event.getTableId());
            return false;
        }
        for (String columnName : table.primaryKeyColumnNames()) {
            int position = LogMinerHelper.getColumnIndexByName(columnName, table);
            Object prevValue = prevEvent.getEntry().getNewValues()[position];
            if (prevValue == null) {
                throw new DebeziumException("Could not find column " + columnName + " in previous event");
            }
            Object value = event.getEntry().getNewValues()[position];
            if (value == null) {
                throw new DebeziumException("Could not find column " + columnName + " in event");
            }
            if (!Objects.equals(value, prevValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merge column values from {@code event} with {@code prevEvent}.
     *
     * @param event current event being processed, never {@code null}
     * @param prevEvent previous/parent parent that has been processed, never {@code null}
     */
    private void mergeNewColumns(LogMinerEvent event, LogMinerEvent prevEvent) {
        final boolean prevEventIsInsert = RowMapper.INSERT == prevEvent.getOperation();

        for (int i = 0; i < event.getEntry().getNewValues().length; ++i) {
            Object value = event.getEntry().getNewValues()[i];
            Object prevValue = prevEvent.getEntry().getNewValues()[i];
            if (prevEventIsInsert && "EMPTY_CLOB()".equals(prevValue)) {
                LOGGER.trace("\tAssigning column index {} with updated CLOB value.", i);
                prevEvent.getEntry().getNewValues()[i] = value;
            }
            else if (prevEventIsInsert && "EMPTY_BLOB()".equals(prevValue)) {
                LOGGER.trace("\tAssigning column index {} with updated BLOB value.", i);
                prevEvent.getEntry().getNewValues()[i] = value;
            }
            else if (!prevEventIsInsert && value != null) {
                LOGGER.trace("\tUpdating column index {} in previous event", i);
                prevEvent.getEntry().getNewValues()[i] = value;
            }
        }
    }

    /**
     * Represents a transaction boundary that was recently committed.
     *
     * This is used by the buffer to detect transactions read from overlapping mining sessions that can
     * safely be ignored as the connector has already reconciled and emitted the event for it.
     */
    private static final class RecentlyCommittedTransaction {
        private final String transactionId;
        private final Scn firstScn;
        private final Scn commitScn;

        public RecentlyCommittedTransaction(Transaction transaction, Scn commitScn) {
            this.transactionId = transaction.transactionId;
            this.firstScn = transaction.firstScn;
            this.commitScn = commitScn;
        }

        public Scn getFirstScn() {
            return firstScn;
        }

        public Scn getCommitScn() {
            return commitScn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RecentlyCommittedTransaction that = (RecentlyCommittedTransaction) o;
            return Objects.equals(transactionId, that.transactionId) &&
                    Objects.equals(firstScn, that.firstScn) &&
                    Objects.equals(commitScn, that.commitScn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionId, firstScn, commitScn);
        }
    }

    /**
     * Represents a logical database transaction
     */
    private static final class Transaction {

        private final String transactionId;
        private final Scn firstScn;
        private Scn lastScn;
        private final Set<Long> eventHashes;
        private final List<LogMinerEvent> events;

        private Transaction(String transactionId, Scn firstScn) {
            this.transactionId = transactionId;
            this.firstScn = firstScn;
            this.events = new ArrayList<>();
            this.eventHashes = new HashSet<>();
            this.lastScn = firstScn;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "transactionId=" + transactionId +
                    ", firstScn=" + firstScn +
                    ", lastScn=" + lastScn +
                    '}';
        }
    }

    /**
     * Base class for all possible LogMiner events
     */
    private static class LogMinerEvent {
        private final int operation;
        private final LogMinerDmlEntry entry;
        private final Scn scn;
        private final TableId tableId;
        private final String rowId;
        private final Object rsId;

        public LogMinerEvent(int operation, LogMinerDmlEntry entry, Scn scn, TableId tableId, String rowId, Object rsId) {
            this.operation = operation;
            this.scn = scn;
            this.tableId = tableId;
            this.rowId = rowId;
            this.rsId = rsId;
            this.entry = entry;
        }

        public int getOperation() {
            return operation;
        }

        public LogMinerDmlEntry getEntry() {
            return entry;
        }

        public Scn getScn() {
            return scn;
        }

        public TableId getTableId() {
            return tableId;
        }

        public String getRowId() {
            return rowId;
        }

        public Object getRsId() {
            return rsId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LogMinerEvent event = (LogMinerEvent) o;
            return operation == event.operation &&
                    Objects.equals(entry, event.entry) &&
                    Objects.equals(scn, event.scn) &&
                    Objects.equals(tableId, event.tableId) &&
                    Objects.equals(rowId, event.rowId) &&
                    Objects.equals(rsId, event.rsId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operation, entry, scn, tableId, rowId, rsId);
        }
    }

    /**
     * Represents a DML event for a given table row.
     */
    private static class DmlEvent extends LogMinerEvent {
        public DmlEvent(int operation, LogMinerDmlEntry entry, Scn scn, TableId tableId, String rowId, Object rsId) {
            super(operation, entry, scn, tableId, rowId, rsId);
        }
    }

    /**
     * Represents a SELECT_LOB_LOCATOR event
     */
    private static class SelectLobLocatorEvent extends LogMinerEvent {
        private final String columnName;
        private final boolean binaryData;

        public SelectLobLocatorEvent(int operation, LogMinerDmlEntry entry, String columnName, boolean binaryData, Scn scn,
                                     TableId tableId, String rowId, Object rsId) {
            super(operation, entry, scn, tableId, rowId, rsId);
            this.columnName = columnName;
            this.binaryData = binaryData;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isBinaryData() {
            return binaryData;
        }
    }

    /**
     * Represents a LOB_WRITE event
     */
    private static class LobWriteEvent extends LogMinerEvent {
        private final String data;

        public LobWriteEvent(int operation, String data, Scn scn, TableId tableId, String rowId, Object rsId) {
            super(operation, null, scn, tableId, rowId, rsId);
            this.data = data;
        }

        public String getData() {
            return data;
        }
    }

    /**
     * Represents a LOB_ERASE event
     */
    private static class LobEraseEvent extends LogMinerEvent {
        public LobEraseEvent(int operation, Scn scn, TableId tableId, String rowId, Object rsId) {
            super(operation, null, scn, tableId, rowId, rsId);
        }
    }

}
