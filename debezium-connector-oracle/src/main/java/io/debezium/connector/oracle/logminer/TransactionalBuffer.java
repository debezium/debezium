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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;

/**
 * Buffer that stores transactions and related callbacks that will be executed when a transaction commits or discarded
 * when a transaction has been rolled back.
 *
 * @author Andrey Pustovetov
 */
@NotThreadSafe
public final class TransactionalBuffer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private final Map<String, Transaction> transactions;
    private final ErrorHandler errorHandler;
    private final Set<String> abandonedTransactionIds;
    private final Set<String> rolledBackTransactionIds;
    private final TransactionalBufferMetrics metrics;

    private Scn lastCommittedScn;

    /**
     * Constructor to create a new instance.
     *
     * @param taskContext the task context
     * @param errorHandler the connector error handler
     */
    TransactionalBuffer(OracleTaskContext taskContext, ErrorHandler errorHandler) {
        this.transactions = new HashMap<>();
        this.errorHandler = errorHandler;
        this.lastCommittedScn = Scn.NULL;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();

        // create metrics and register them
        this.metrics = new TransactionalBufferMetrics(taskContext);
        this.metrics.register(LOGGER);
    }

    /**
     * @return the transactional buffer's metrics
     */
    TransactionalBufferMetrics getMetrics() {
        return metrics;
    }

    /**
     * @return rolled back transactions
     */
    Set<String> getRolledBackTransactionIds() {
        return new HashSet<>(rolledBackTransactionIds);
    }

    /**
     * Registers callback to execute when transaction commits.
     *
     * @param transactionId transaction identifier
     * @param scn           SCN
     * @param changeTime    time of DML parsing completion
     * @param callback      callback to execute when transaction commits
     */
    void registerCommitCallback(String transactionId, Scn scn, Instant changeTime, CommitCallback callback) {
        if (abandonedTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(metrics, "Captured DML for abandoned transaction {}, ignored", transactionId);
            return;
        }
        // this should never happen
        if (rolledBackTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(metrics, "Captured DML for rolled-back transaction {}, ignored", transactionId);
            return;
        }

        transactions.computeIfAbsent(transactionId, s -> new Transaction(scn));

        metrics.setActiveTransactions(transactions.size());
        metrics.incrementRegisteredDmlCounter();
        metrics.calculateLagMetrics(changeTime);

        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            transaction.commitCallbacks.add(callback);
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
                   ChangeEventSource.ChangeEventSourceContext context, String debugMessage, EventDispatcher dispatcher) {

        Instant start = Instant.now();
        Transaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            return false;
        }

        Scn smallestScn = calculateSmallestScn();

        abandonedTransactionIds.remove(transactionId);

        // On the restarting connector, we start from SCN in the offset. There is possibility to commit a transaction(s) which were already committed.
        // Currently we cannot use ">=", because we may lose normal commit which may happen at the same time. TODO use audit table to prevent duplications
        if ((offsetContext.getCommitScn() != null && offsetContext.getCommitScn().compareTo(scn) > 0) || lastCommittedScn.compareTo(scn) > 0) {
            LogMinerHelper.logWarn(metrics,
                    "Transaction {} was already processed, ignore. Committed SCN in offset is {}, commit SCN of the transaction is {}, last committed SCN is {}",
                    transactionId, offsetContext.getCommitScn(), scn, lastCommittedScn);
            metrics.setActiveTransactions(transactions.size());
            return false;
        }

        List<CommitCallback> commitCallbacks = transaction.commitCallbacks;
        LOGGER.trace("COMMIT, {}, smallest SCN: {}", debugMessage, smallestScn);
        commit(context, offsetContext, start, commitCallbacks, timestamp, smallestScn, scn, dispatcher);

        return true;
    }

    private void commit(ChangeEventSource.ChangeEventSourceContext context, OracleOffsetContext offsetContext, Instant start,
                        List<CommitCallback> commitCallbacks, Timestamp timestamp, Scn smallestScn, Scn scn, EventDispatcher<?> dispatcher) {
        try {
            int counter = commitCallbacks.size();
            for (CommitCallback callback : commitCallbacks) {
                if (!context.isRunning()) {
                    return;
                }
                callback.execute(timestamp, smallestScn, scn, --counter);
            }

            lastCommittedScn = Scn.valueOf(scn.longValue());

            if (!commitCallbacks.isEmpty()) {
                dispatcher.dispatchTransactionCommittedEvent(offsetContext);
            }
        }
        catch (InterruptedException e) {
            LogMinerHelper.logError(metrics, "Thread interrupted during running", e);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            metrics.incrementCommittedTransactions();
            metrics.setActiveTransactions(transactions.size());
            metrics.incrementCommittedDmlCounter(commitCallbacks.size());
            metrics.setCommittedScn(scn);
            metrics.setOffsetScn(offsetContext.getScn());
            metrics.setLastCommitDuration(Duration.between(start, Instant.now()).toMillis());
        }
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

            metrics.setActiveTransactions(transactions.size());
            metrics.incrementRolledBackTransactions();
            metrics.addRolledBackTransactionId(transactionId);

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
        LogMinerHelper.logWarn(metrics, "All transactions with first SCN <= {} will be abandoned, offset: {}", thresholdScn, offsetContext.getScn());
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
                LogMinerHelper.logWarn(metrics, "Following long running transaction {} will be abandoned and ignored: {} ", transaction.getKey(),
                        transaction.getValue().toString());
                abandonedTransactionIds.add(transaction.getKey());
                iter.remove();

                metrics.addAbandonedTransactionId(transaction.getKey());
                metrics.setActiveTransactions(transactions.size());
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
        metrics.setOldestScn(scn == null ? Scn.valueOf(-1) : scn);
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

    /**
     * Set the database time difference.
     *
     * @param difference the time difference in milliseconds
     */
    void setDatabaseTimeDifference(long difference) {
        metrics.setTimeDifference(new AtomicLong(difference));
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

        if (this.metrics != null) {
            // if metrics registered, unregister them
            this.metrics.unregister(LOGGER);
        }
    }

    /**
     * Callback is designed to execute when transaction commits.
     */
    public interface CommitCallback {

        /**
         * Executes callback.
         *
         * @param timestamp      commit timestamp
         * @param smallestScn    smallest SCN among other transactions
         * @param commitScn      commit SCN
         * @param callbackNumber number of the callback in the transaction
         */
        void execute(Timestamp timestamp, Scn smallestScn, Scn commitScn, int callbackNumber) throws InterruptedException;
    }

    @NotThreadSafe
    private static final class Transaction {

        private final Scn firstScn;
        private Scn lastScn;
        private final List<CommitCallback> commitCallbacks;

        private Transaction(Scn firstScn) {
            this.firstScn = firstScn;
            this.commitCallbacks = new ArrayList<>();
            this.lastScn = firstScn;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "firstScn=" + firstScn +
                    ", lastScn=" + lastScn +
                    '}';
        }
    }
}
