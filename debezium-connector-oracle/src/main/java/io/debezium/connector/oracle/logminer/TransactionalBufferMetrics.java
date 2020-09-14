/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

/**
 * This class contains MBean methods
 */
@ThreadSafe
public class TransactionalBufferMetrics extends Metrics implements TransactionalBufferMetricsMXBean {
    private final AtomicLong oldestScn = new AtomicLong();
    private final AtomicLong committedScn = new AtomicLong();
    private final AtomicReference<Duration> lagFromTheSource = new AtomicReference<>();
    private final AtomicInteger activeTransactions = new AtomicInteger();
    private final AtomicLong rolledBackTransactions = new AtomicLong();
    private final AtomicLong committedTransactions = new AtomicLong();
    private final AtomicLong capturedDmlCounter = new AtomicLong();
    private final AtomicLong committedDmlCounter = new AtomicLong();
    private final AtomicInteger commitQueueCapacity = new AtomicInteger();
    private final AtomicReference<Duration> maxLagFromTheSource = new AtomicReference<>();
    private final AtomicReference<Duration> minLagFromTheSource = new AtomicReference<>();
    private final AtomicReference<Duration> averageLagsFromTheSource = new AtomicReference<>();
    private final AtomicReference<Set<String>> abandonedTransactionIds = new AtomicReference<>();
    private final AtomicReference<Set<String>> rolledBackTransactionIds = new AtomicReference<>();
    private final Instant startTime;
    private final static long MILLIS_PER_SECOND = 1000L;
    private final AtomicLong timeDifference = new AtomicLong();
    private final AtomicInteger errorCounter = new AtomicInteger();
    private final AtomicInteger warningCounter = new AtomicInteger();
    private final AtomicInteger scnFreezeCounter = new AtomicInteger();

    TransactionalBufferMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner-transactional-buffer");
        startTime = Instant.now();
        oldestScn.set(-1);
        committedScn.set(-1);
        timeDifference.set(0);
        reset();
    }

    // setters
    void setOldestScn(Long scn) {
        oldestScn.set(scn);
    }

    public void setCommittedScn(Long scn) {
        committedScn.set(scn);
    }

    public void setTimeDifference(AtomicLong timeDifference) {
        this.timeDifference.set(timeDifference.get());
    }

    void calculateLagMetrics(Instant changeTime) {
        if (changeTime != null) {
            Instant correctedChangeTime = changeTime.plus(Duration.ofMillis(timeDifference.longValue()));
            lagFromTheSource.set(Duration.between(correctedChangeTime, Instant.now()).abs());

            if (maxLagFromTheSource.get().toMillis() < lagFromTheSource.get().toMillis()) {
                maxLagFromTheSource.set(lagFromTheSource.get());
            }
            if (minLagFromTheSource.get().toMillis() > lagFromTheSource.get().toMillis()) {
                minLagFromTheSource.set(lagFromTheSource.get());
            }

            if (averageLagsFromTheSource.get().isZero()) {
                averageLagsFromTheSource.set(lagFromTheSource.get());
            }
            else {
                averageLagsFromTheSource.set(averageLagsFromTheSource.get().plus(lagFromTheSource.get()).dividedBy(2));
            }
        }
    }

    void setActiveTransactions(Integer counter) {
        if (counter != null) {
            activeTransactions.set(counter);
        }
    }

    void incrementRolledBackTransactions() {
        rolledBackTransactions.incrementAndGet();
    }

    void incrementCommittedTransactions() {
        committedTransactions.incrementAndGet();
    }

    void incrementCapturedDmlCounter() {
        capturedDmlCounter.incrementAndGet();
    }

    void incrementCommittedDmlCounter(int counter) {
        committedDmlCounter.getAndAdd(counter);
    }

    void addAbandonedTransactionId(String transactionId) {
        if (transactionId != null) {
            abandonedTransactionIds.get().add(transactionId);
        }
    }

    void addRolledBackTransactionId(String transactionId) {
        if (transactionId != null) {
            rolledBackTransactionIds.get().add(transactionId);
        }
    }

    /**
     * This is to increase logged logError counter.
     * There are other ways to monitor the log, but this is just to check if there are any.
     */
    void incrementErrorCounter() {
        errorCounter.incrementAndGet();
    }

    /**
     * This is to increase logged warning counter
     * There are other ways to monitor the log, but this is just to check if there are any.
     */
    void incrementWarningCounter() {
        warningCounter.incrementAndGet();
    }

    /**
     * This counter to accumulate number of encountered observations when SCN does not change in the offset.
     * This call indicates an uncommitted oldest transaction in the buffer.
     */
    void incrementScnFreezeCounter() {
        scnFreezeCounter.incrementAndGet();
    }

    // implemented getters
    @Override
    public Long getOldestScn() {
        return oldestScn.get();
    }

    @Override
    public Long getCommittedScn() {
        return committedScn.get();
    }

    @Override
    public int getNumberOfActiveTransactions() {
        return activeTransactions.get();
    }

    @Override
    public long getNumberOfRolledBackTransactions() {
        return rolledBackTransactions.get();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return committedTransactions.get();
    }

    @Override
    public long getCommitThroughput() {
        long timeSpent = Duration.between(startTime, Instant.now()).isZero() ? 1 : Duration.between(startTime, Instant.now()).toMillis();
        return committedTransactions.get() * MILLIS_PER_SECOND / timeSpent;
    }

    @Override
    public long getCapturedDmlThroughput() {
        long timeSpent = Duration.between(startTime, Instant.now()).isZero() ? 1 : Duration.between(startTime, Instant.now()).toMillis();
        return committedDmlCounter.get() * MILLIS_PER_SECOND / timeSpent;
    }

    @Override
    public long getCapturedDmlCount() {
        return capturedDmlCounter.longValue();
    }

    @Override
    public long getLagFromSource() {
        return lagFromTheSource.get().toMillis();
    }

    @Override
    public long getMaxLagFromSource() {
        return maxLagFromTheSource.get().toMillis();
    }

    @Override
    public long getMinLagFromSource() {
        return minLagFromTheSource.get().toMillis();
    }

    @Override
    public long getAverageLagFromSource() {
        return averageLagsFromTheSource.get().toMillis();
    }

    @Override
    public Set<String> getAbandonedTransactionIds() {
        return abandonedTransactionIds.get();
    }

    @Override
    public Set<String> getRolledBackTransactionIds() {
        return rolledBackTransactionIds.get();
    }

    @Override
    public int getErrorCounter() {
        return errorCounter.get();
    }

    @Override
    public int getWarningCounter() {
        return warningCounter.get();
    }

    @Override
    public int getScnFreezeCounter() {
        return scnFreezeCounter.get();
    }

    @Override
    public int getCommitQueueCapacity() {
        return commitQueueCapacity.get();
    }

    void setCommitQueueCapacity(int commitQueueCapacity) {
        this.commitQueueCapacity.set(commitQueueCapacity);
    }

    @Override
    public void reset() {
        maxLagFromTheSource.set(Duration.ZERO);
        minLagFromTheSource.set(Duration.ZERO);
        averageLagsFromTheSource.set(Duration.ZERO);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
        committedTransactions.set(0);
        capturedDmlCounter.set(0);
        committedDmlCounter.set(0);
        abandonedTransactionIds.set(new HashSet<>());
        rolledBackTransactionIds.set(new HashSet<>());
        lagFromTheSource.set(Duration.ZERO);
        errorCounter.set(0);
        warningCounter.set(0);
        scnFreezeCounter.set(0);
        commitQueueCapacity.set(0);
    }

    @Override
    public String toString() {
        return "TransactionalBufferMetrics{" +
                "oldestScn=" + oldestScn.get() +
                ", committedScn=" + committedScn.get() +
                ", lagFromTheSource=" + lagFromTheSource.get() +
                ", activeTransactions=" + activeTransactions.get() +
                ", rolledBackTransactions=" + rolledBackTransactions.get() +
                ", committedTransactions=" + committedTransactions.get() +
                ", capturedDmlCounter=" + capturedDmlCounter.get() +
                ", committedDmlCounter=" + committedDmlCounter.get() +
                ", maxLagFromTheSource=" + maxLagFromTheSource.get() +
                ", minLagFromTheSource=" + minLagFromTheSource.get() +
                ", averageLagsFromTheSource=" + averageLagsFromTheSource.get() +
                ", abandonedTransactionIds=" + abandonedTransactionIds.get() +
                ", errorCounter=" + errorCounter.get() +
                ", warningCounter=" + warningCounter.get() +
                ", scnFreezeCounter=" + scnFreezeCounter.get() +
                ", commitQueueCapacity=" + commitQueueCapacity.get() +
                '}';
    }
}
