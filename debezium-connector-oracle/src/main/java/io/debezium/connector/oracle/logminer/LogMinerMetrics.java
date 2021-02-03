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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.metrics.Metrics;

/**
 * This class contains methods to be exposed via MBean server
 *
 */
@ThreadSafe
public class LogMinerMetrics extends Metrics implements LogMinerMetricsMXBean {

    private final static int DEFAULT_HOURS_TO_KEEP_TRANSACTION = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerMetrics.class);

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
    private final AtomicLong millisecondToSleepBetweenMiningQuery = new AtomicLong();

    private final AtomicBoolean recordMiningHistory = new AtomicBoolean();
    private final AtomicInteger hoursToKeepTransaction = new AtomicInteger();
    private final AtomicLong networkConnectionProblemsCounter = new AtomicLong();

    // Constants for sliding window algorithm
    private final int batchSizeMin;
    private final int batchSizeMax;
    private final int batchSizeDefault;

    // constants for sleeping algorithm
    private final long sleepTimeMin;
    private final long sleepTimeMax;
    private final long sleepTimeDefault;
    private final long sleepTimeIncrement;

    LogMinerMetrics(CdcSourceTaskContext taskContext, OracleConnectorConfig connectorConfig) {
        super(taskContext, "log-miner");

        currentScn.set(-1);
        currentLogFileName = new AtomicReference<>();
        redoLogStatus = new AtomicReference<>();
        switchCounter.set(0);

        batchSizeDefault = connectorConfig.getLogMiningBatchSizeDefault();
        batchSizeMin = connectorConfig.getLogMiningBatchSizeMin();
        batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();

        sleepTimeDefault = connectorConfig.getLogMiningSleepTimeDefault().toMillis();
        sleepTimeMin = connectorConfig.getLogMiningSleepTimeMin().toMillis();
        sleepTimeMax = connectorConfig.getLogMiningSleepTimeMax().toMillis();
        sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();

        reset();
        LOGGER.info("Logminer metrics initialized {}", this);
    }

    @Override
    public void reset() {

        batchSize.set(batchSizeDefault);
        millisecondToSleepBetweenMiningQuery.set(sleepTimeDefault);
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
        return millisecondToSleepBetweenMiningQuery.intValue();
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
        if (size >= batchSizeMin && size <= batchSizeMax) {
            batchSize.set(size);
        }
    }

    @Override
    public void setMillisecondToSleepBetweenMiningQuery(Integer milliseconds) {
        if (milliseconds != null && milliseconds >= sleepTimeMin && milliseconds < sleepTimeMax) {
            millisecondToSleepBetweenMiningQuery.set(milliseconds);
        }
    }

    @Override
    public void changeSleepingTime(boolean increment) {
        long sleepTime = millisecondToSleepBetweenMiningQuery.get();
        if (increment && sleepTime < sleepTimeMax) {
            sleepTime = millisecondToSleepBetweenMiningQuery.addAndGet(sleepTimeIncrement);
        }
        else if (sleepTime > sleepTimeMin) {
            sleepTime = millisecondToSleepBetweenMiningQuery.addAndGet(-sleepTimeIncrement);
        }

        LOGGER.debug("Updating sleep time window. Sleep time {}. Min sleep time {}. Max sleep time {}.", sleepTime, sleepTimeMin, sleepTimeMax);
    }

    public void changeBatchSize(boolean increment) {
        
        int currentBatchSize = batchSize.get();
        if (increment && currentBatchSize < batchSizeMax) {
            currentBatchSize = batchSize.addAndGet(batchSizeMin);
        }
        else if (currentBatchSize > batchSizeMin) {
            currentBatchSize = batchSize.addAndGet(-batchSizeMin);
        }

        if (currentBatchSize == batchSizeMax) {
            LOGGER.info("LogMiner is now using the maximum batch size {}. This could be indicative of large SCN gaps", currentBatchSize);
        } else {
            LOGGER.debug("Updating batch size window. Batch size {}. Min batch size {}. Max batch size {}.", currentBatchSize, batchSizeMin, batchSizeMax);
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
                ", batchSizeDefault=" + batchSizeDefault +
                ", batchSizeMin=" + batchSizeMin +
                ", batchSizeMax=" + batchSizeMax +
                ", sleepTimeDefault=" + sleepTimeDefault +
                ", sleepTimeMin=" + sleepTimeMin +
                ", sleepTimeMax=" + sleepTimeMax + 
                ", sleepTimeIncrement=" + sleepTimeIncrement +
                '}';
    }
}
