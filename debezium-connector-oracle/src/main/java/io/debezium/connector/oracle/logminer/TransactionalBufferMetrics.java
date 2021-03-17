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

    private final static long MILLIS_PER_SECOND = 1000L;

    private final AtomicReference<Scn> oldestScn = new AtomicReference<>();
    private final AtomicReference<Scn> committedScn = new AtomicReference<>();
    private final AtomicReference<Scn> offsetScn = new AtomicReference<>();
    private final AtomicInteger activeTransactions = new AtomicInteger();
    private final AtomicLong rolledBackTransactions = new AtomicLong();
    private final AtomicLong committedTransactions = new AtomicLong();
    private final AtomicLong registeredDmlCounter = new AtomicLong();
    private final AtomicLong committedDmlCounter = new AtomicLong();
    private final AtomicLong lastCommitDuration = new AtomicLong();
    private final AtomicLong maxCommitDuration = new AtomicLong();
    private final AtomicReference<Duration> lagFromTheSource = new AtomicReference<>();
    private final AtomicReference<Duration> maxLagFromTheSource = new AtomicReference<>();
    private final AtomicReference<Duration> minLagFromTheSource = new AtomicReference<>();
    private final AtomicReference<Set<String>> abandonedTransactionIds = new AtomicReference<>();
    private final AtomicReference<Set<String>> rolledBackTransactionIds = new AtomicReference<>();
    private final Instant startTime;
    private final AtomicLong timeDifference = new AtomicLong();
    private final AtomicInteger errorCounter = new AtomicInteger();
    private final AtomicInteger warningCounter = new AtomicInteger();
    private final AtomicInteger scnFreezeCounter = new AtomicInteger();

    TransactionalBufferMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner-transactional-buffer");
        startTime = Instant.now();
        oldestScn.set(Scn.INVALID);
        committedScn.set(Scn.INVALID);
        timeDifference.set(0);
        offsetScn.set(Scn.ZERO);
        reset();
    }

    // setters
    void setOldestScn(Scn scn) {
        oldestScn.set(scn);
    }

    public void setCommittedScn(Scn scn) {
        committedScn.set(scn);
    }

    public void setOffsetScn(Scn scn) {
        offsetScn.set(scn);
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

    void incrementRegisteredDmlCounter() {
        registeredDmlCounter.incrementAndGet();
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

    void setLastCommitDuration(Long lastDuration) {
        lastCommitDuration.set(lastDuration);
        if (lastDuration > maxCommitDuration.get()) {
            maxCommitDuration.set(lastDuration);
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
        return oldestScn.get().longValue();
    }

    @Override
    public Long getCommittedScn() {
        return committedScn.get().longValue();
    }

    @Override
    public Long getOffsetScn() {
        return offsetScn.get().longValue();
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
        long timeSpent = Duration.between(startTime, Instant.now()).toMillis();
        return committedTransactions.get() * MILLIS_PER_SECOND / (timeSpent != 0 ? timeSpent : 1);
    }

    @Override
    public long getRegisteredDmlCount() {
        return registeredDmlCounter.longValue();
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

    public Long getLastCommitDuration() {
        return lastCommitDuration.get();
    }

    public Long getMaxCommitDuration() {
        return maxCommitDuration.get();
    }

    @Override
    public void reset() {
        maxLagFromTheSource.set(Duration.ZERO);
        minLagFromTheSource.set(Duration.ZERO);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
        committedTransactions.set(0);
        registeredDmlCounter.set(0);
        committedDmlCounter.set(0);
        abandonedTransactionIds.set(new HashSet<>());
        rolledBackTransactionIds.set(new HashSet<>());
        lagFromTheSource.set(Duration.ZERO);
        errorCounter.set(0);
        warningCounter.set(0);
        scnFreezeCounter.set(0);
    }

    @Override
    public String toString() {
        return "TransactionalBufferMetrics{" +
                "oldestScn=" + oldestScn.get() +
                ", committedScn=" + committedScn.get() +
                ", offsetScn=" + offsetScn.get() +
                ", lagFromTheSource=" + lagFromTheSource.get() +
                ", activeTransactions=" + activeTransactions.get() +
                ", rolledBackTransactions=" + rolledBackTransactions.get() +
                ", committedTransactions=" + committedTransactions.get() +
                ", lastCommitDuration=" + lastCommitDuration +
                ", maxCommitDuration=" + maxCommitDuration +
                ", registeredDmlCounter=" + registeredDmlCounter.get() +
                ", committedDmlCounter=" + committedDmlCounter.get() +
                ", maxLagFromTheSource=" + maxLagFromTheSource.get() +
                ", minLagFromTheSource=" + minLagFromTheSource.get() +
                ", abandonedTransactionIds=" + abandonedTransactionIds.get() +
                ", errorCounter=" + errorCounter.get() +
                ", warningCounter=" + warningCounter.get() +
                ", scnFreezeCounter=" + scnFreezeCounter.get() +
                '}';
    }
}
