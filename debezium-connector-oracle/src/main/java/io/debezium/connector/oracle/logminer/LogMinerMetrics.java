/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

/**
 * This class contains methods to be exposed via MBean server
 *
 */
@ThreadSafe
public class LogMinerMetrics extends Metrics implements LogMinerMetricsMXBean {

    public final static int DEFAULT_BATCH_SIZE = 20_000;

    private final static int MAX_SLEEP_TIME = 3_000;
    private final static int DEFAULT_SLEEP_TIME = 1_000;
    private final static int MIN_SLEEP_TIME = 0;

    private final static int MIN_BATCH_SIZE = 1_000;
    private final static int MAX_BATCH_SIZE = 100_000;
    private final static int DEFAULT_HOURS_TO_KEEP_TRANSACTION = 4;

    private final static int SLEEP_TIME_INCREMENT = 200;

    private final AtomicLong currentScn = new AtomicLong();
    private final AtomicInteger logMinerQueryCount = new AtomicInteger();
    private final AtomicInteger totalCapturedDmlCount = new AtomicInteger();
    private final AtomicReference<Duration> totalDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicInteger lastCapturedDmlCount = new AtomicInteger();
    private final AtomicReference<Duration> lastDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicLong maxCapturedDmlCount = new AtomicLong();
    private final AtomicReference<Duration> maxDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicReference<Duration> totalBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxBatchProcessingDuration = new AtomicReference<>();
    private final AtomicLong maxBatchProcessingThroughput = new AtomicLong();
    private final AtomicReference<String[]> currentLogFileName;
    private final AtomicReference<String[]> redoLogStatus;
    private final AtomicInteger switchCounter = new AtomicInteger();

    private final AtomicInteger batchSize = new AtomicInteger();
    private final AtomicInteger millisecondToSleepBetweenMiningQuery = new AtomicInteger();

    private final AtomicBoolean recordMiningHistory = new AtomicBoolean();
    private final AtomicInteger hoursToKeepTransaction = new AtomicInteger();
    private final AtomicLong networkConnectionProblemsCounter = new AtomicLong();

    LogMinerMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner");

        // todo: this was removed, is that accurate?
        // batchSize.set(DEFAULT_BATCH_SIZE);
        // millisecondToSleepBetweenMiningQuery.set(DEFAULT_SLEEP_TIME);

        currentScn.set(-1);
        currentLogFileName = new AtomicReference<>();
        redoLogStatus = new AtomicReference<>();
        switchCounter.set(0);

        reset();
    }

    @Override
    public void reset() {
        batchSize.set(DEFAULT_BATCH_SIZE);
        millisecondToSleepBetweenMiningQuery.set(DEFAULT_SLEEP_TIME);
        totalCapturedDmlCount.set(0);
        maxDurationOfFetchingQuery.set(Duration.ZERO);
        lastDurationOfFetchingQuery.set(Duration.ZERO);
        logMinerQueryCount.set(0);
        hoursToKeepTransaction.set(DEFAULT_HOURS_TO_KEEP_TRANSACTION);
        maxBatchProcessingDuration.set(Duration.ZERO);
        totalDurationOfFetchingQuery.set(Duration.ZERO);
        lastCapturedDmlCount.set(0);
        maxCapturedDmlCount.set(0);
        totalBatchProcessingDuration.set(Duration.ZERO);
        maxBatchProcessingThroughput.set(0);
        lastBatchProcessingDuration.set(Duration.ZERO);
        networkConnectionProblemsCounter.set(0);
    }

    // setters
    public void setCurrentScn(Long scn) {
        currentScn.set(scn);
    }

    public void setCurrentLogFileName(Set<String> names) {
        currentLogFileName.set(names.stream().toArray(String[]::new));
    }

    public void setRedoLogStatus(Map<String, String> status) {
        String[] statusArray = status.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        redoLogStatus.set(statusArray);
    }

    public void setSwitchCount(int counter) {
        switchCounter.set(counter);
    }

    public void setLastCapturedDmlCount(int dmlCount) {
        lastCapturedDmlCount.set(dmlCount);
        if (dmlCount > maxCapturedDmlCount.get()) {
            maxCapturedDmlCount.set(dmlCount);
        }
        totalCapturedDmlCount.getAndAdd(dmlCount);
    }

    public void setLastDurationOfBatchCapturing(Duration lastDuration) {
        lastDurationOfFetchingQuery.set(lastDuration);
        totalDurationOfFetchingQuery.accumulateAndGet(lastDurationOfFetchingQuery.get(), Duration::plus);
        if (maxDurationOfFetchingQuery.get().toMillis() < lastDurationOfFetchingQuery.get().toMillis()) {
            maxDurationOfFetchingQuery.set(lastDuration);
        }
        logMinerQueryCount.incrementAndGet();
    }

    public void setLastDurationOfBatchProcessing(Duration lastDuration) {
        lastBatchProcessingDuration.set(lastDuration);
        totalBatchProcessingDuration.accumulateAndGet(lastDuration, Duration::plus);
        if (maxBatchProcessingDuration.get().toMillis() < lastDuration.toMillis()) {
            maxBatchProcessingDuration.set(lastDuration);
        }
        if (getLastBatchProcessingThroughput() > maxBatchProcessingThroughput.get()) {
            maxBatchProcessingThroughput.set(getLastBatchProcessingThroughput());
        }
    }

    public void incrementNetworkConnectionProblemsCounter() {
        networkConnectionProblemsCounter.incrementAndGet();
    }

    // implemented getters
    @Override
    public Long getCurrentScn() {
        return currentScn.get();
    }

    @Override
    public long getTotalCapturedDmlCount() {
        return totalCapturedDmlCount.get();
    }

    @Override
    public String[] getCurrentRedoLogFileName() {
        return currentLogFileName.get();
    }

    @Override
    public String[] getRedoLogStatus() {
        return redoLogStatus.get();
    }

    @Override
    public int getSwitchCounter() {
        return switchCounter.get();
    }

    @Override
    public Long getLastDurationOfFetchingQuery() {
        return lastDurationOfFetchingQuery.get() == null ? 0 : lastDurationOfFetchingQuery.get().toMillis();
    }

    @Override
    public long getLastBatchProcessingDuration() {
        return lastBatchProcessingDuration.get().toMillis();
    }

    @Override
    public Long getMaxDurationOfFetchingQuery() {
        return maxDurationOfFetchingQuery.get() == null ? 0 : maxDurationOfFetchingQuery.get().toMillis();
    }

    @Override
    public Long getMaxCapturedDmlInBatch() {
        return maxCapturedDmlCount.get();
    }

    @Override
    public int getLastCapturedDmlCount() {
        return lastCapturedDmlCount.get();
    }

    @Override
    public long getAverageBatchProcessingThroughput() {
        if (totalBatchProcessingDuration.get().isZero()) {
            return 0L;
        }
        return Math.round((totalCapturedDmlCount.floatValue() / totalBatchProcessingDuration.get().toMillis()) * 1000);
    }

    @Override
    public long getLastBatchProcessingThroughput() {
        if (lastBatchProcessingDuration.get().isZero()) {
            return 0L;
        }
        return Math.round((lastCapturedDmlCount.floatValue() / lastBatchProcessingDuration.get().toMillis()) * 1000);
    }

    @Override
    public long getFetchingQueryCount() {
        return logMinerQueryCount.get();
    }

    @Override
    public int getBatchSize() {
        return batchSize.get();
    }

    @Override
    public Integer getMillisecondToSleepBetweenMiningQuery() {
        return millisecondToSleepBetweenMiningQuery.get();
    }

    @Override
    public boolean getRecordMiningHistory() {
        return recordMiningHistory.get();
    }

    @Override
    public int getHoursToKeepTransactionInBuffer() {
        return hoursToKeepTransaction.get();
    }

    @Override
    public long getMaxBatchProcessingThroughput() {
        return maxBatchProcessingThroughput.get();
    }

    @Override
    public long getNetworkConnectionProblemsCounter() {
        return networkConnectionProblemsCounter.get();
    }

    // MBean accessible setters
    @Override
    public void setBatchSize(int size) {
        if (size >= MIN_BATCH_SIZE && size <= MAX_BATCH_SIZE) {
            batchSize.set(size);
        }
    }

    @Override
    public void setMillisecondToSleepBetweenMiningQuery(Integer milliseconds) {
        if (milliseconds != null && milliseconds >= MIN_SLEEP_TIME && milliseconds < MAX_SLEEP_TIME) {
            millisecondToSleepBetweenMiningQuery.set(milliseconds);
        }
    }

    @Override
    public void changeSleepingTime(boolean increment) {
        int sleepTime = millisecondToSleepBetweenMiningQuery.get();
        if (increment && sleepTime < MAX_SLEEP_TIME) {
            millisecondToSleepBetweenMiningQuery.getAndAdd(SLEEP_TIME_INCREMENT);
        }
        else if (sleepTime > MIN_SLEEP_TIME) {
            millisecondToSleepBetweenMiningQuery.getAndAdd(-SLEEP_TIME_INCREMENT);
        }
    }

    public void changeBatchSize(boolean increment) {
        if (increment && batchSize.get() < MAX_BATCH_SIZE) {
            batchSize.getAndAdd(MIN_BATCH_SIZE);
        }
        else if (batchSize.get() > MIN_BATCH_SIZE) {
            batchSize.getAndAdd(-MIN_BATCH_SIZE);
        }
    }

    @Override
    public void setRecordMiningHistory(boolean doRecording) {
        recordMiningHistory.set(doRecording);
    }

    @Override
    public void setHoursToKeepTransactionInBuffer(int hours) {
        if (hours > 0 && hours <= 48) {
            hoursToKeepTransaction.set(hours);
        }
    }

    @Override
    public String toString() {
        return "LogMinerMetrics{" +
                "currentScn=" + currentScn +
                ", logMinerQueryCount=" + logMinerQueryCount +
                ", totalCapturedDmlCount=" + totalCapturedDmlCount +
                ", totalDurationOfFetchingQuery=" + totalDurationOfFetchingQuery +
                ", lastCapturedDmlCount=" + lastCapturedDmlCount +
                ", lastDurationOfFetchingQuery=" + lastDurationOfFetchingQuery +
                ", maxCapturedDmlCount=" + maxCapturedDmlCount +
                ", maxDurationOfFetchingQuery=" + maxDurationOfFetchingQuery +
                ", totalBatchProcessingDuration=" + totalBatchProcessingDuration +
                ", lastBatchProcessingDuration=" + lastBatchProcessingDuration +
                ", maxBatchProcessingDuration=" + maxBatchProcessingDuration +
                ", maxBatchProcessingThroughput=" + maxBatchProcessingThroughput +
                ", currentLogFileName=" + currentLogFileName +
                ", redoLogStatus=" + redoLogStatus +
                ", switchCounter=" + switchCounter +
                ", batchSize=" + batchSize +
                ", millisecondToSleepBetweenMiningQuery=" + millisecondToSleepBetweenMiningQuery +
                ", recordMiningHistory=" + recordMiningHistory +
                ", hoursToKeepTransaction=" + hoursToKeepTransaction +
                ", networkConnectionProblemsCounter" + networkConnectionProblemsCounter +
                '}';
    }
}
