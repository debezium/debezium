/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Threads;

/**
 * @author Andrey Pustovetov
 * <p>
 * Transactional buffer is designed to register callbacks, to execute them when transaction commits and to clear them
 * when transaction rollbacks.
 */
@NotThreadSafe
public final class TransactionalBuffer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private final Map<String, Transaction> transactions;
    private final ExecutorService executor;
    private final AtomicInteger taskCounter;
    private final ErrorHandler errorHandler;
    private final Supplier<Integer> commitQueueCapacity;
    private TransactionalBufferMetrics metrics;
    private final Set<String> abandonedTransactionIds;
    private final Set<String> rolledBackTransactionIds;

    // It holds the latest captured SCN.
    // This number tracks starting point for the next mining cycle.
    private BigDecimal largestScn;
    private BigDecimal lastCommittedScn;

    /**
     * Constructor to create a new instance.
     *
     * @param logicalName           logical name
     * @param errorHandler          logError handler
     * @param metrics               metrics MBean
     * @param inCommitQueueCapacity commit queue capacity. On overflow, caller runs task
     */
    TransactionalBuffer(String logicalName, ErrorHandler errorHandler, TransactionalBufferMetrics metrics, int inCommitQueueCapacity) {
        this.transactions = new HashMap<>();
        final BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(inCommitQueueCapacity);
        executor = new ThreadPoolExecutor(1, 1,
                Integer.MAX_VALUE, TimeUnit.MILLISECONDS,
                workQueue,
                Threads.threadFactory(OracleConnector.class, logicalName, "transactional-buffer", true, false),
                new ThreadPoolExecutor.CallerRunsPolicy());
        commitQueueCapacity = workQueue::remainingCapacity;
        this.taskCounter = new AtomicInteger();
        this.errorHandler = errorHandler;
        this.metrics = metrics;
        largestScn = BigDecimal.ZERO;
        lastCommittedScn = BigDecimal.ZERO;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();
    }

    /**
     * @return largest last SCN in the buffer among all transactions
     */
    BigDecimal getLargestScn() {
        return largestScn;
    }

    /**
     * @return rolled back transactions
     */
    Set<String> getRolledBackTransactionIds() {
        return new HashSet<>(rolledBackTransactionIds);
    }

    /**
     * Reset Largest SCN
     */
    void resetLargestScn(Long value) {
        if (value != null) {
            largestScn = new BigDecimal(value);
        }
        else {
            largestScn = BigDecimal.ZERO;
        }
    }

    /**
     * Registers callback to execute when transaction commits.
     *
     * @param transactionId transaction identifier
     * @param scn           SCN
     * @param changeTime    time of DML parsing completion
     * @param callback      callback to execute when transaction commits
     */
    void registerCommitCallback(String transactionId, BigDecimal scn, Instant changeTime, CommitCallback callback) {
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

        if (scn.compareTo(largestScn) > 0) {
            largestScn = scn;
        }
    }

    /**
     * If the commit executor queue is full, back-pressure will be applied by letting execution of the callback
     * be performed by the calling thread.
     *
     * @param transactionId transaction identifier
     * @param scn           SCN of the commit.
     * @param offsetContext Oracle offset
     * @param timestamp     commit timestamp
     * @param context       context to check that source is running
     * @param debugMessage  message
     * @return true if committed transaction is in the buffer, was not processed yet and processed now
     */
    boolean commit(String transactionId, BigDecimal scn, OracleOffsetContext offsetContext, Timestamp timestamp,
                   ChangeEventSource.ChangeEventSourceContext context, String debugMessage) {

        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return false;
        }

        Instant start = Instant.now();

        calculateLargestScn();
        transaction = transactions.remove(transactionId);
        BigDecimal smallestScn = calculateSmallestScn();

        taskCounter.incrementAndGet();
        abandonedTransactionIds.remove(transactionId);

        // On the restarting connector, we start from SCN in the offset. There is possibility to commit a transaction(s) which were already committed.
        // Currently we cannot use ">=", because we may lose normal commit which may happen at the same time. TODO use audit table to prevent duplications
        if ((offsetContext.getCommitScn() != null && offsetContext.getCommitScn() > scn.longValue()) || lastCommittedScn.longValue() > scn.longValue()) {
            LogMinerHelper.logWarn(metrics,
                    "Transaction {} was already processed, ignore. Committed SCN in offset is {}, commit SCN of the transaction is {}, last committed SCN is {}",
                    transactionId, offsetContext.getCommitScn(), scn, lastCommittedScn);
            metrics.setActiveTransactions(transactions.size());
            return false;
        }

        List<CommitCallback> commitCallbacks = transaction.commitCallbacks;
        LOGGER.trace("COMMIT, {}, smallest SCN: {}, largest SCN {}", debugMessage, smallestScn, largestScn);
        executor.execute(() -> {
            try {
                int counter = commitCallbacks.size();
                for (CommitCallback callback : commitCallbacks) {
                    if (!context.isRunning()) {
                        return;
                    }
                    callback.execute(timestamp, smallestScn, scn, --counter);
                }

                lastCommittedScn = new BigDecimal(scn.longValue());
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
                metrics.setCommittedScn(scn.longValue());
                metrics.setOffsetScn(offsetContext.getScn());
                metrics.setCommitQueueCapacity(commitQueueCapacity.get());
                metrics.setLastCommitDuration(Duration.between(start, Instant.now()).toMillis());
                taskCounter.decrementAndGet();
            }
        });
        metrics.setCommitQueueCapacity(commitQueueCapacity.get());

        return true;
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

            calculateLargestScn(); // in case if largest SCN was in this transaction
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
     */
    void abandonLongTransactions(Long thresholdScn) {
        BigDecimal threshold = new BigDecimal(thresholdScn);
        BigDecimal smallestScn = calculateSmallestScn();
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
                calculateLargestScn();

                metrics.addAbandonedTransactionId(transaction.getKey());
                metrics.setActiveTransactions(transactions.size());
            }
        }
    }

    boolean isTransactionRegistered(String txId) {
        return transactions.get(txId) != null;
    }

    private BigDecimal calculateSmallestScn() {
        BigDecimal scn = transactions.isEmpty() ? null
                : transactions.values()
                        .stream()
                        .map(transaction -> transaction.firstScn)
                        .min(BigDecimal::compareTo)
                        .orElseThrow(() -> new DataException("Cannot calculate smallest SCN"));
        metrics.setOldestScn(scn == null ? -1 : scn.longValue());
        return scn;
    }

    private void calculateLargestScn() {
        largestScn = transactions.isEmpty() ? BigDecimal.ZERO
                : transactions.values()
                        .stream()
                        .map(transaction -> transaction.lastScn)
                        .max(BigDecimal::compareTo)
                        .orElseThrow(() -> new DataException("Cannot calculate largest SCN"));
    }

    /**
     * Returns {@code true} if buffer is empty, otherwise {@code false}.
     *
     * @return {@code true} if buffer is empty, otherwise {@code false}
     */
    boolean isEmpty() {
        return transactions.isEmpty() && taskCounter.get() == 0;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        this.transactions.values().forEach(t -> result.append(t.toString()));
        return result.toString();
    }

    /**
     * Closes buffer.
     */
    void close() {
        transactions.clear();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            LogMinerHelper.logError(metrics, "Thread interrupted during shutdown", e);
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
        void execute(Timestamp timestamp, BigDecimal smallestScn, BigDecimal commitScn, int callbackNumber) throws InterruptedException;
    }

    @NotThreadSafe
    private static final class Transaction {

        private final BigDecimal firstScn;
        private BigDecimal lastScn;
        private final List<CommitCallback> commitCallbacks;

        private Transaction(BigDecimal firstScn) {
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
