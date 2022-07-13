/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import com.alibaba.fastjson.JSON;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.util.DmlEventJsonUtil;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Buffer that stores transactions and related callbacks that will be executed when a transaction commits or discarded
 * when a transaction has been rolled back.
 *
 * @author Andrey Pustovetov
 */
@NotThreadSafe
public final class TransactionalBuffer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private static final String ORACLE_BIG_TRANSACTIONAL = "oracle_big_transactional";

    private final Map<String, Transaction> transactions;
    private final OracleDatabaseSchema schema;
    private final Clock clock;
    private final ErrorHandler errorHandler;
    private final Set<String> abandonedTransactionIds;
    private final Set<String> rolledBackTransactionIds;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    private String bigTransactionalCachePath;
    private int bigTransactionalLimitCount;

    private Scn lastCommittedScn;

    /**
     * Constructor to create a new instance.
     *
     * @param errorHandler the connector error handler
     * @param streamingMetrics the streaming metrics
     */
    TransactionalBuffer(OracleDatabaseSchema schema, Clock clock, ErrorHandler errorHandler, OracleStreamingChangeEventSourceMetrics streamingMetrics,
                        String bigTransactionalCachePath, int bigTransactionalLimitCount) {
        this.transactions = new HashMap<>();
        this.schema = schema;
        this.clock = clock;
        this.errorHandler = errorHandler;
        this.lastCommittedScn = Scn.NULL;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();
        this.streamingMetrics = streamingMetrics;
        this.bigTransactionalCachePath = bigTransactionalCachePath;
        this.bigTransactionalLimitCount = bigTransactionalLimitCount;
    }

    TransactionalBuffer(OracleDatabaseSchema schema, Clock clock, ErrorHandler errorHandler, OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.transactions = new HashMap<>();
        this.schema = schema;
        this.clock = clock;
        this.errorHandler = errorHandler;
        this.lastCommittedScn = Scn.NULL;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();
        this.streamingMetrics = streamingMetrics;
    }

    /**
     * @return rolled back transactions
     */
    Set<String> getRolledBackTransactionIds() {
        return new HashSet<>(rolledBackTransactionIds);
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
     */
    void registerDmlOperation(int operation, String transactionId, Scn scn, TableId tableId, LogMinerDmlEntry parseEntry, Instant changeTime, String rowId) {
        if (abandonedTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(streamingMetrics, "Captured DML for abandoned transaction {}, ignored.", transactionId);
            return;
        }
        if (rolledBackTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(streamingMetrics, "Captured DML for rolled back transaction {}, ignored.", transactionId);
            return;
        }

        initOrFillTransaction(operation, transactionId, scn, tableId, parseEntry, changeTime, rowId);

        streamingMetrics.setActiveTransactions(transactions.size());
        streamingMetrics.incrementRegisteredDmlCount();
        streamingMetrics.calculateLagMetrics(changeTime);
    }

    private void initOrFillTransaction(int operation, String transactionId, Scn scn, TableId tableId, LogMinerDmlEntry parseEntry, Instant changeTime, String rowId) {
        try {
            final Transaction transaction = transactions.computeIfAbsent(transactionId, s -> new Transaction(transactionId, scn));
            // 如果大事物限制没有开启，或者没满足大事物条件，则直接add
            if (transaction.getFileOutputStream() == null &&
                    (bigTransactionalLimitCount < 1 || (transaction.events.size() + 1) < bigTransactionalLimitCount)) {
                transaction.events.add(new DmlEvent(operation, parseEntry, scn, tableId, rowId));
                return;
            }
            // 初始化写出文件流
            initFileOutputStream(transaction, getCachePath(bigTransactionalCachePath), getCacheFileName(transactionId, scn, tableId, changeTime));
            // 写出事件
            writeOutEvents(transaction, new DmlEvent(operation, parseEntry, scn, tableId, rowId));
        }
        catch (Exception e) {
            throw new RuntimeException("qgg exception", e);
        }
    }

    private String getCacheFileName(String transactionId, Scn scn, TableId tableId, Instant changeTime) {
        return String.join("_", tableId.catalog(), tableId.schema(), tableId.table(), transactionId,
                String.valueOf(scn.longValue()), String.valueOf(changeTime.toEpochMilli()));
    }

    private void writeOutEvents(Transaction transaction, DmlEvent dmlEvent) throws IOException {
        // 判断是否为首次写出
        if (transaction.events.isEmpty()) {
            transaction.getFileOutputStream().write(generateEventRecord(dmlEvent));
            transaction.getFileOutputStream().flush();
            return;
        }
        for (DmlEvent oldEvent : transaction.events) {
            transaction.getFileOutputStream().write(generateEventRecord(oldEvent));
        }
        transaction.getFileOutputStream().write(generateEventRecord(dmlEvent));
        transaction.getFileOutputStream().flush();
        transaction.events.clear();
    }

    private byte[] generateEventRecord(DmlEvent dmlEvent) {
        final StringBuilder record = new StringBuilder();
        final String lineSeparator = System.getProperty("line.separator");
        record.append("-- ").append(DmlEventJsonUtil.serialization(dmlEvent))
                .append(lineSeparator)
                .append(parseCreateDml(dmlEvent))
                .append(lineSeparator);
        return record.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String parseCreateDml(DmlEvent dmlEvent) {
        final LogMinerDmlEntry dmlEntry = dmlEvent.getEntry();
        if (dmlEntry == null) {
            return "null dmlEntry";
        }
        switch (dmlEntry.getCommandType()) {
            case READ:
                return "read event not support";
            case CREATE:
                return insertDml(dmlEntry);
            case UPDATE:
                return updateDml(dmlEntry);
            case DELETE:
                return deleteDml(dmlEntry);
            case TRUNCATE:
                return "truncate event not support";
            default:
                return "noMatchingEventTypeFound";
        }
    }

    private String deleteDml(LogMinerDmlEntry dmlEntry) {
        final StringBuilder deleteDml = new StringBuilder();
        deleteDml.append("delete from ")
                .append(dmlEntry.getObjectOwner())
                .append(".")
                .append(dmlEntry.getObjectName())
                .append(" where ");
        for (LogMinerColumnValue oldValue : dmlEntry.getOldValues()) {
            deleteDml.append(" ")
                    .append(oldValue.getColumnName())
                    .append("=")
                    .append(oldValue.getColumnData())
                    .append(" and");
        }
        if (deleteDml.lastIndexOf("and") == deleteDml.length() - 3) {
            deleteDml.delete(deleteDml.length() - 3, deleteDml.length());
        }
        deleteDml.append(";");
        return deleteDml.toString();
    }

    private String updateDml(LogMinerDmlEntry dmlEntry) {
        final StringBuilder updateDml = new StringBuilder();
        updateDml.append("-- old: ");
        for (LogMinerColumnValue oldValue : dmlEntry.getOldValues()) {
            updateDml.append(oldValue.getColumnName())
                    .append("=")
                    .append(oldValue.getColumnData())
                    .append(" ");
        }
        updateDml.append(System.getProperty("line.separator"))
                .append(insertDml(dmlEntry));
        return updateDml.toString();
    }

    private String insertDml(LogMinerDmlEntry dmlEntry) {
        final StringBuilder createDml = new StringBuilder();
        createDml.append("insert into ")
                .append(dmlEntry.getObjectOwner())
                .append(".")
                .append(dmlEntry.getObjectName())
                .append(valueJoin(dmlEntry))
                .append(";");
        return createDml.toString();
    }

    private String valueJoin(LogMinerDmlEntry dmlEntry) {
        final StringBuilder name = new StringBuilder(" (");
        final StringBuilder value = new StringBuilder(" VALUES (");
        for (LogMinerColumnValue newValue : dmlEntry.getNewValues()) {
            valueJoinAsItem(name, value, newValue);
        }
        if (name.lastIndexOf(",") == name.length() - 1) {
            name.deleteCharAt(name.length() - 1);
        }
        if (value.lastIndexOf(",") == value.length() - 1) {
            value.deleteCharAt(value.length() - 1);
        }
        name.append(")").append(value).append(")");
        return name.toString();
    }

    private void valueJoinAsItem(StringBuilder name, StringBuilder value, LogMinerColumnValue newValue) {
        if (newValue == null || newValue.getColumnName() == null) {
            return;
        }
        name.append(newValue.getColumnName()).append(",");
        if (newValue.getColumnData() == null || (!(newValue.getColumnData() instanceof String))) {
            value.append(newValue.getColumnData());
        }
        else {
            value.append("'").append(newValue.getColumnData()).append("'").append(",");
        }
    }

    private void initFileOutputStream(Transaction transaction, String cachePath, String cacheFileName) throws Exception {
        if (transaction.getFileOutputStream() != null) {
            return;
        }
        final File cacheDir = new File(cachePath);
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
        else {
            clearExpiredFile(cacheDir);
        }
        final String cacheFilePath = cachePath + File.separator + cacheFileName;
        final File cacheFile = new File(cacheFilePath);
        if (!cacheFile.exists()) {
            cacheFile.createNewFile();
        }
        transaction.setCacheFilePath(cacheFilePath);
        transaction.setFileOutputStream(new FileOutputStream(cacheFile));
    }

    private void clearExpiredFile(File cacheDir) {
        for (File file : cacheDir.listFiles()) {
            final LocalDateTime lastModified = LocalDateTime.ofInstant(Instant.ofEpochMilli(file.lastModified()), ZoneId.systemDefault());
            if (lastModified.plusDays(7).compareTo(LocalDateTime.now()) > 0) {
                continue;
            }
            file.delete();
        }
    }

    public static String getCachePath(String bigTransactionalCachePath) {
        if (bigTransactionalCachePath == null || bigTransactionalCachePath.isEmpty()) {
            return System.getProperty("user.dir") + File.separator + ORACLE_BIG_TRANSACTIONAL;
        }
        return bigTransactionalCachePath + File.separator + ORACLE_BIG_TRANSACTIONAL;
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
                   ChangeEventSource.ChangeEventSourceContext context, String debugMessage, EventDispatcher<TableId> dispatcher)
            throws IOException {

        Instant start = Instant.now();
        Transaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            return false;
        }

        preCommit(transactionId, scn, timestamp, debugMessage, transaction);

        Scn smallestScn = calculateSmallestScn();

        abandonedTransactionIds.remove(transactionId);

        // On the restarting connector, we start from SCN in the offset. There is possibility to commit a transaction(s) which were already committed.
        // Currently we cannot use ">=", because we may lose normal commit which may happen at the same time. TODO use audit table to prevent duplications
        if ((offsetContext.getCommitScn() != null && offsetContext.getCommitScn().compareTo(scn) > 0) || lastCommittedScn.compareTo(scn) > 0) {
            LogMinerHelper.logWarn(streamingMetrics,
                    "Transaction {} was already processed, ignore. Committed SCN in offset is {}, commit SCN of the transaction is {}, last committed SCN is {}",
                    transactionId, offsetContext.getCommitScn(), scn, lastCommittedScn);
            streamingMetrics.setActiveTransactions(transactions.size());
            return false;
        }

        LOGGER.trace("COMMIT, {}, smallest SCN: {}", debugMessage, smallestScn);
        commit(context, offsetContext, start, transaction, timestamp, smallestScn, scn, dispatcher);

        return true;
    }

    private void preCommit(String transactionId, Scn scn, Timestamp timestamp, String debugMessage, Transaction transaction) throws IOException {
        if (transaction.getFileOutputStream() == null) {
            return;
        }
        final String commitMessage = "--end transaction commit, transactionId=" + transactionId + ", scn=" + scn.longValue()
                + ", timestamp=" + timestamp.getTime() + ", debugMessage=" + debugMessage;
        transaction.getFileOutputStream().write(commitMessage.getBytes(StandardCharsets.UTF_8));
        transaction.getFileOutputStream().flush();
        transaction.getFileOutputStream().close();
        throw new RuntimeException("exist big transaction, transactionId=" + transactionId + ", transaction_file=" + transaction.getCacheFilePath());
    }

    private void commit(ChangeEventSource.ChangeEventSourceContext context, OracleOffsetContext offsetContext, Instant start,
                        Transaction transaction, Timestamp timestamp, Scn smallestScn, Scn scn, EventDispatcher<TableId> dispatcher) {
        try {
            int counter = transaction.events.size();
            for (DmlEvent event : transaction.events) {
                if (!context.isRunning()) {
                    return;
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

                LOGGER.trace("Processing DML event {} with SCN {}", event.getEntry(), event.getScn());
                dispatcher.dispatchDataChangeEvent(event.getTableId(),
                        new LogMinerChangeRecordEmitter(
                                offsetContext,
                                event.getEntry(),
                                schema.tableFor(event.getTableId()),
                                clock));
            }

            lastCommittedScn = Scn.valueOf(scn.longValue());

            if (!transaction.events.isEmpty()) {
                dispatcher.dispatchTransactionCommittedEvent(offsetContext);
            }
        }
        catch (InterruptedException e) {
            LogMinerHelper.logError(streamingMetrics, "Thread interrupted during running", e);
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
    }

    /**
     * Clears registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction id
     * @param debugMessage  message
     * @return true if the rollback is for a transaction in the buffer
     */
    boolean rollback(String transactionId, String debugMessage) throws Exception {

        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            LOGGER.debug("Transaction rolled back: {}", debugMessage);

            preRollback(transactionId, debugMessage, transaction);

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

    private void preRollback(String transactionId, String debugMessage, Transaction transaction) throws IOException {
        if (transaction.getFileOutputStream() == null) {
            return;
        }
        transaction.getFileOutputStream().write(("--end transaction rollback, transactionId=" + transactionId + ", debugMessage=" + debugMessage)
                .getBytes(StandardCharsets.UTF_8));
        transaction.getFileOutputStream().flush();
        transaction.getFileOutputStream().close();
        final File file = new File(transaction.getCacheFilePath());
        if (file.exists()) {
            file.delete();
        }
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

        if (this.streamingMetrics != null) {
            // if metrics registered, unregister them
            this.streamingMetrics.unregister(LOGGER);
        }
    }

    /**
     * Represents a logical database transaction
     */
    private static final class Transaction {

        private final String transactionId;
        private final Scn firstScn;
        private Scn lastScn;
        private final List<DmlEvent> events;

        private String cacheFilePath;

        private FileOutputStream fileOutputStream;

        private Transaction(String transactionId, Scn firstScn) {
            this.transactionId = transactionId;
            this.firstScn = firstScn;
            this.events = new ArrayList<>();
            this.lastScn = firstScn;
        }

        public void setFileOutputStream(FileOutputStream fileOutputStream) {
            this.fileOutputStream = fileOutputStream;
        }

        public FileOutputStream getFileOutputStream() {
            return fileOutputStream;
        }

        public String getCacheFilePath() {
            return cacheFilePath;
        }

        public void setCacheFilePath(String cacheFilePath) {
            this.cacheFilePath = cacheFilePath;
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
     * Represents a DML event for a given table row.
     */
    public static class DmlEvent {
        private final int operation;
        private final LogMinerDmlEntry entry;
        private final Scn scn;
        private final TableId tableId;
        private final String rowId;

        public DmlEvent(int operation, LogMinerDmlEntry entry, Scn scn, TableId tableId, String rowId) {
            this.operation = operation;
            this.scn = scn;
            this.tableId = tableId;
            this.rowId = rowId;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DmlEvent dmlEvent = (DmlEvent) o;
            return operation == dmlEvent.operation &&
                    Objects.equals(entry, dmlEvent.entry) &&
                    Objects.equals(scn, dmlEvent.scn) &&
                    Objects.equals(tableId, dmlEvent.tableId) &&
                    Objects.equals(rowId, dmlEvent.rowId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operation, entry, scn, tableId, rowId);
        }
    }
}
