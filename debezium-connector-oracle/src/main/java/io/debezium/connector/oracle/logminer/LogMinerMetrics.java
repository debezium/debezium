/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
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

    private final static int MAX_SLEEP_TIME = 3_000;
    private final static int DEFAULT_SLEEP_TIME = 1_000;
    private final static int MIN_SLEEP_TIME = 100;

    private final static int MIN_BATCH_SIZE = 1_000;
    private final static int MAX_BATCH_SIZE = 100_000;
    private final static int DEFAULT_BATCH_SIZE = 5_000;

    private final static int SLEEP_TIME_INCREMENT = 200;

    private final AtomicLong currentScn = new AtomicLong();
    private final AtomicInteger capturedDmlCount = new AtomicInteger();
    private final AtomicReference<String[]> currentLogFileName;
    private final AtomicReference<String[]> redoLogStatus;
    private final AtomicInteger switchCounter = new AtomicInteger();
    private final AtomicReference<Duration> lastLogMinerQueryDuration = new AtomicReference<>();
    private final AtomicReference<Duration> averageLogMinerQueryDuration = new AtomicReference<>();
    private final AtomicInteger logMinerQueryCount = new AtomicInteger();
    private final AtomicReference<Duration> lastProcessedCapturedBatchDuration = new AtomicReference<>();
    private final AtomicInteger processedCapturedBatchCount = new AtomicInteger();
    private final AtomicReference<Duration> averageProcessedCapturedBatchDuration = new AtomicReference<>();
    private final AtomicInteger batchSize = new AtomicInteger();
    private final AtomicInteger millisecondToSleepBetweenMiningQuery = new AtomicInteger();

    LogMinerMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner");

        batchSize.set(DEFAULT_BATCH_SIZE);
        millisecondToSleepBetweenMiningQuery.set(DEFAULT_SLEEP_TIME);

        currentScn.set(-1);
        capturedDmlCount.set(0);
        currentLogFileName = new AtomicReference<>();
        redoLogStatus = new AtomicReference<>();
        switchCounter.set(0);
        averageLogMinerQueryDuration.set(Duration.ZERO);
        lastLogMinerQueryDuration.set(Duration.ZERO);
        logMinerQueryCount.set(0);
        lastProcessedCapturedBatchDuration.set(Duration.ZERO);
        processedCapturedBatchCount.set(0);
        averageProcessedCapturedBatchDuration.set(Duration.ZERO);
    }

    // setters
    public void setCurrentScn(Long scn) {
        currentScn.set(scn);
    }

    public void incrementCapturedDmlCount() {
        capturedDmlCount.incrementAndGet();
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

    public void setLastLogMinerQueryDuration(Duration fetchDuration) {
        setDurationMetrics(fetchDuration, lastLogMinerQueryDuration, logMinerQueryCount, averageLogMinerQueryDuration);
    }

    public void setProcessedCapturedBatchDuration(Duration processDuration) {
        setDurationMetrics(processDuration, lastProcessedCapturedBatchDuration, processedCapturedBatchCount, averageProcessedCapturedBatchDuration);
    }

    // implemented getters
    @Override
    public Long getCurrentScn() {
        return currentScn.get();
    }

    @Override
    public int getCapturedDmlCount() {
        return capturedDmlCount.get();
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
    public Long getLastLogMinerQueryDuration() {
        return lastLogMinerQueryDuration.get() == null ? 0 : lastLogMinerQueryDuration.get().toMillis();
    }

    @Override
    public Long getAverageLogMinerQueryDuration() {
        return averageLogMinerQueryDuration.get() == null ? 0 : averageLogMinerQueryDuration.get().toMillis();
    }

    @Override
    public Long getLastProcessedCapturedBatchDuration() {
        return lastProcessedCapturedBatchDuration.get() == null ? 0 : lastProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getLogMinerQueryCount() {
        return logMinerQueryCount.get();
    }

    @Override
    public int getProcessedCapturedBatchCount() {
        return processedCapturedBatchCount.get();
    }

    @Override
    public Long getAverageProcessedCapturedBatchDuration() {
        return averageProcessedCapturedBatchDuration.get() == null ? 0 : averageProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getBatchSize() {
        return batchSize.get();
    }

    @Override
    public Integer getMillisecondToSleepBetweenMiningQuery() {
        return millisecondToSleepBetweenMiningQuery.get();
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
        int change = increment ? SLEEP_TIME_INCREMENT : -SLEEP_TIME_INCREMENT;
        if (sleepTime >= MIN_SLEEP_TIME && sleepTime < MAX_SLEEP_TIME) {
            millisecondToSleepBetweenMiningQuery.getAndAdd(change);
        }
    }

    // private methods
    private void setDurationMetrics(Duration duration, AtomicReference<Duration> lastDuration, AtomicInteger counter,
                                    AtomicReference<Duration> averageDuration) {
        if (duration != null) {
            lastDuration.set(duration);
            int count = counter.incrementAndGet();
            Duration currentAverage = averageDuration.get() == null ? Duration.ZERO : averageDuration.get();
            averageDuration.set(currentAverage.multipliedBy(count - 1).plus(duration).dividedBy(count));
        }
    }

    @Override
    public String toString() {
        return "LogMinerMetrics{" +
                "currentEndScn=" + currentScn.get() +
                ", currentLogFileNames=" + Arrays.toString(currentLogFileName.get()) +
                ", redoLogStatus=" + Arrays.toString(redoLogStatus.get()) +
                ", capturedDmlCount=" + capturedDmlCount.get() +
                ", switchCounter=" + switchCounter.get() +
                ", lastLogMinerQueryDuration=" + lastLogMinerQueryDuration.get() +
                ", logMinerQueryCount=" + logMinerQueryCount.get() +
                ", averageLogMinerQueryDuration=" + averageLogMinerQueryDuration.get() +
                ", lastProcessedCapturedBatchDuration=" + lastProcessedCapturedBatchDuration.get() +
                ", processedCapturedBatchCount=" + processedCapturedBatchCount.get() +
                ", averageProcessedCapturedBatchDuration=" + averageProcessedCapturedBatchDuration.get() +
                ", millisecondToSleepBetweenMiningQuery=" + millisecondToSleepBetweenMiningQuery.get() +
                ", batchSize=" + batchSize.get() +
                '}';
    }
}
