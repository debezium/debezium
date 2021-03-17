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

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerMetrics.class);

    private final AtomicReference<Scn> currentScn = new AtomicReference<>();
    private final AtomicInteger logMinerQueryCount = new AtomicInteger();
    private final AtomicInteger totalCapturedDmlCount = new AtomicInteger();
    private final AtomicReference<Duration> totalDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicInteger lastCapturedDmlCount = new AtomicInteger();
    private final AtomicReference<Duration> lastDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicLong maxCapturedDmlCount = new AtomicLong();
    private final AtomicLong totalProcessedRows = new AtomicLong();
    private final AtomicReference<Duration> maxDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicReference<Duration> totalBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> totalParseTime = new AtomicReference<>();
    private final AtomicReference<Duration> totalStartLogMiningSessionDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastStartLogMiningSessionDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxStartingLogMiningSessionDuration = new AtomicReference<>();
    private final AtomicReference<Duration> totalProcessingTime = new AtomicReference<>();
    private final AtomicReference<Duration> minBatchProcessingTime = new AtomicReference<>();
    private final AtomicReference<Duration> maxBatchProcessingTime = new AtomicReference<>();
    private final AtomicReference<Duration> totalResultSetNextTime = new AtomicReference<>();
    private final AtomicLong maxBatchProcessingThroughput = new AtomicLong();
    private final AtomicReference<String[]> currentLogFileName;
    private final AtomicReference<String[]> redoLogStatus;
    private final AtomicLong minimumLogsMined = new AtomicLong();
    private final AtomicLong maximumLogsMined = new AtomicLong();
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

        currentScn.set(Scn.INVALID);
        currentLogFileName = new AtomicReference<>();
        minimumLogsMined.set(0L);
        maximumLogsMined.set(0L);
        redoLogStatus = new AtomicReference<>();
        switchCounter.set(0);

        recordMiningHistory.set(connectorConfig.isLogMiningHistoryRecorded());
        batchSizeDefault = connectorConfig.getLogMiningBatchSizeDefault();
        batchSizeMin = connectorConfig.getLogMiningBatchSizeMin();
        batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();

        sleepTimeDefault = connectorConfig.getLogMiningSleepTimeDefault().toMillis();
        sleepTimeMin = connectorConfig.getLogMiningSleepTimeMin().toMillis();
        sleepTimeMax = connectorConfig.getLogMiningSleepTimeMax().toMillis();
        sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();

        hoursToKeepTransaction.set(Long.valueOf(connectorConfig.getLogMiningTransactionRetention().toHours()).intValue());

        reset();
        LOGGER.info("Logminer metrics initialized {}", this);
    }

    @Override
    public void reset() {
        batchSize.set(batchSizeDefault);
        millisecondToSleepBetweenMiningQuery.set(sleepTimeDefault);
        totalCapturedDmlCount.set(0);
        totalProcessedRows.set(0);
        maxDurationOfFetchingQuery.set(Duration.ZERO);
        lastDurationOfFetchingQuery.set(Duration.ZERO);
        logMinerQueryCount.set(0);
        maxBatchProcessingDuration.set(Duration.ZERO);
        totalDurationOfFetchingQuery.set(Duration.ZERO);
        lastCapturedDmlCount.set(0);
        maxCapturedDmlCount.set(0);
        totalBatchProcessingDuration.set(Duration.ZERO);
        maxBatchProcessingThroughput.set(0);
        lastBatchProcessingDuration.set(Duration.ZERO);
        networkConnectionProblemsCounter.set(0);
        totalParseTime.set(Duration.ZERO);
        totalStartLogMiningSessionDuration.set(Duration.ZERO);
        lastStartLogMiningSessionDuration.set(Duration.ZERO);
        maxStartingLogMiningSessionDuration.set(Duration.ZERO);
        totalProcessingTime.set(Duration.ZERO);
        minBatchProcessingTime.set(Duration.ZERO);
        maxBatchProcessingTime.set(Duration.ZERO);
        totalResultSetNextTime.set(Duration.ZERO);
    }

    public void setCurrentScn(Scn scn) {
        currentScn.set(scn);
    }

    public void setCurrentLogFileName(Set<String> names) {
        currentLogFileName.set(names.stream().toArray(String[]::new));
        if (names.size() < minimumLogsMined.get()) {
            minimumLogsMined.set(names.size());
        }
        if (names.size() > maximumLogsMined.get()) {
            maximumLogsMined.set(names.size());
        }
    }

    @Override
    public long getMinimumMinedLogCount() {
        return minimumLogsMined.get();
    }

    @Override
    public long getMaximumMinedLogCount() {
        return maximumLogsMined.get();
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

    @Override
    public Long getCurrentScn() {
        return currentScn.get().longValue();
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
    public Long getLastDurationOfFetchQueryInMilliseconds() {
        return lastDurationOfFetchingQuery.get() == null ? 0 : lastDurationOfFetchingQuery.get().toMillis();
    }

    @Override
    public long getLastBatchProcessingTimeInMilliseconds() {
        return lastBatchProcessingDuration.get().toMillis();
    }

    @Override
    public Long getMaxDurationOfFetchQueryInMilliseconds() {
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
    public long getTotalProcessedRows() {
        return totalProcessedRows.get();
    }

    @Override
    public long getTotalResultSetNextTimeInMilliseconds() {
        return totalResultSetNextTime.get().toMillis();
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
    public long getMillisecondToSleepBetweenMiningQuery() {
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

    @Override
    public long getTotalParseTimeInMilliseconds() {
        return totalParseTime.get().toMillis();
    }

    public void addCurrentParseTime(Duration currentParseTime) {
        totalParseTime.accumulateAndGet(currentParseTime, Duration::plus);
    }

    @Override
    public long getTotalMiningSessionStartTimeInMilliseconds() {
        return totalStartLogMiningSessionDuration.get().toMillis();
    }

    public void addCurrentMiningSessionStart(Duration currentStartLogMiningSession) {
        lastStartLogMiningSessionDuration.set(currentStartLogMiningSession);
        if (currentStartLogMiningSession.compareTo(maxStartingLogMiningSessionDuration.get()) > 0) {
            maxStartingLogMiningSessionDuration.set(currentStartLogMiningSession);
        }
        totalStartLogMiningSessionDuration.accumulateAndGet(currentStartLogMiningSession, Duration::plus);
    }

    @Override
    public long getLastMiningSessionStartTimeInMilliseconds() {
        return lastStartLogMiningSessionDuration.get().toMillis();
    }

    @Override
    public long getMaxMiningSessionStartTimeInMilliseconds() {
        return maxStartingLogMiningSessionDuration.get().toMillis();
    }

    @Override
    public long getTotalProcessingTimeInMilliseconds() {
        return totalProcessingTime.get().toMillis();
    }

    @Override
    public long getMinBatchProcessingTimeInMilliseconds() {
        return minBatchProcessingTime.get().toMillis();
    }

    @Override
    public long getMaxBatchProcessingTimeInMilliseconds() {
        return maxBatchProcessingTime.get().toMillis();
    }

    public void setCurrentBatchProcessingTime(Duration currentBatchProcessingTime) {
        totalProcessingTime.accumulateAndGet(currentBatchProcessingTime, Duration::plus);
    }

    public void addCurrentResultSetNext(Duration currentNextTime) {
        totalResultSetNextTime.accumulateAndGet(currentNextTime, Duration::plus);
    }

    public void addProcessedRows(Long rows) {
        totalProcessedRows.getAndAdd(rows);
    }

    @Override
    public void setBatchSize(int size) {
        if (size >= batchSizeMin && size <= batchSizeMax) {
            batchSize.set(size);
        }
    }

    @Override
    public void setMillisecondToSleepBetweenMiningQuery(long milliseconds) {
        if (milliseconds >= sleepTimeMin && milliseconds < sleepTimeMax) {
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
        }
        else {
            LOGGER.debug("Updating batch size window. Batch size {}. Min batch size {}. Max batch size {}.", currentBatchSize, batchSizeMin, batchSizeMax);
        }
    }

    @Override
    public String toString() {
        return "LogMinerMetrics{" +
                "currentScn=" + currentScn +
                ", logMinerQueryCount=" + logMinerQueryCount +
                ", totalProcessedRows=" + totalProcessedRows +
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
                ", minLogFilesMined=" + minimumLogsMined +
                ", maxLogFilesMined=" + maximumLogsMined +
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
                ", totalParseTime=" + totalParseTime +
                ", totalStartLogMiningSessionDuration=" + totalStartLogMiningSessionDuration +
                ", lastStartLogMiningSessionDuration=" + lastStartLogMiningSessionDuration +
                ", maxStartLogMiningSessionDuration=" + maxStartingLogMiningSessionDuration +
                ", totalProcessTime=" + totalProcessingTime +
                ", minBatchProcessTime=" + minBatchProcessingTime +
                ", maxBatchProcessTime=" + maxBatchProcessingTime +
                ", totalResultSetNextTime=" + totalResultSetNextTime +
                '}';
    }
}
