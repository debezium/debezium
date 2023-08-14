/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.LRUCacheMap;

/**
 * The metrics implementation for Oracle connector streaming phase.
 */
@ThreadSafe
public class OracleStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<OraclePartition>
        implements OracleStreamingChangeEventSourceMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleStreamingChangeEventSourceMetrics.class);

    private static final long MILLIS_PER_SECOND = 1000L;
    private static final int TRANSACTION_ID_SET_SIZE = 10;

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

    private final AtomicLong networkConnectionProblemsCounter = new AtomicLong();

    private final AtomicReference<Duration> keepTransactionsDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lagFromTheSourceDuration = new AtomicReference<>();
    private final AtomicReference<Duration> minLagFromTheSourceDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxLagFromTheSourceDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastCommitDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxCommitDuration = new AtomicReference<>();
    private final AtomicLong activeTransactions = new AtomicLong();
    private final AtomicLong rolledBackTransactions = new AtomicLong();
    private final AtomicLong committedTransactions = new AtomicLong();
    private final AtomicLong oversizedTransactions = new AtomicLong();
    private final AtomicReference<LRUCacheMap<String, String>> abandonedTransactionIds = new AtomicReference<>();
    private final AtomicReference<LRUCacheMap<String, String>> rolledBackTransactionIds = new AtomicReference<>();
    private final AtomicLong registeredDmlCount = new AtomicLong();
    private final AtomicLong committedDmlCount = new AtomicLong();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger warningCount = new AtomicInteger();
    private final AtomicInteger scnFreezeCount = new AtomicInteger();
    private final AtomicLong timeDifference = new AtomicLong();
    private final AtomicReference<ZoneOffset> zoneOffset = new AtomicReference<>();
    private final AtomicReference<Scn> oldestScn = new AtomicReference<>();
    private final AtomicReference<Scn> committedScn = new AtomicReference<>();
    private final AtomicReference<Scn> offsetScn = new AtomicReference<>();
    private final AtomicInteger unparsableDdlCount = new AtomicInteger();
    private final AtomicLong miningSessionUserGlobalAreaMemory = new AtomicLong();
    private final AtomicLong miningSessionUserGlobalAreaMaxMemory = new AtomicLong();
    private final AtomicLong miningSessionProcessGlobalAreaMemory = new AtomicLong();
    private final AtomicLong miningSessionProcessGlobalAreaMaxMemory = new AtomicLong();

    // Constants for sliding window algorithm
    private final int batchSizeMin;
    private final int batchSizeMax;
    private final int batchSizeDefault;

    // constants for sleeping algorithm
    private final long sleepTimeMin;
    private final long sleepTimeMax;
    private final long sleepTimeDefault;
    private final long sleepTimeIncrement;

    private final Instant startTime;

    private final Clock clock;

    public OracleStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                   EventMetadataProvider metadataProvider,
                                                   OracleConnectorConfig connectorConfig) {
        this(taskContext, changeEventQueueMetrics, metadataProvider, connectorConfig, Clock.systemUTC());
    }

    /**
     * Constructor that allows providing a clock to be used for Tests.
     */
    @VisibleForTesting
    OracleStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                            EventMetadataProvider metadataProvider,
                                            OracleConnectorConfig connectorConfig,
                                            Clock clock) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);

        this.clock = clock;
        startTime = clock.instant();
        timeDifference.set(0L);
        zoneOffset.set(ZoneOffset.UTC);

        currentScn.set(Scn.NULL);
        oldestScn.set(Scn.NULL);
        offsetScn.set(Scn.NULL);
        committedScn.set(Scn.NULL);

        currentLogFileName = new AtomicReference<>(new String[0]);
        minimumLogsMined.set(0L);
        maximumLogsMined.set(0L);
        redoLogStatus = new AtomicReference<>(new String[0]);
        switchCounter.set(0);

        batchSizeDefault = connectorConfig.getLogMiningBatchSizeDefault();
        batchSizeMin = connectorConfig.getLogMiningBatchSizeMin();
        batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();

        sleepTimeDefault = connectorConfig.getLogMiningSleepTimeDefault().toMillis();
        sleepTimeMin = connectorConfig.getLogMiningSleepTimeMin().toMillis();
        sleepTimeMax = connectorConfig.getLogMiningSleepTimeMax().toMillis();
        sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();

        keepTransactionsDuration.set(connectorConfig.getLogMiningTransactionRetention());

        reset();
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
        miningSessionUserGlobalAreaMemory.set(0L);
        miningSessionUserGlobalAreaMaxMemory.set(0L);
        miningSessionProcessGlobalAreaMemory.set(0L);
        miningSessionProcessGlobalAreaMaxMemory.set(0L);

        // transactional buffer metrics
        lagFromTheSourceDuration.set(Duration.ZERO);
        maxLagFromTheSourceDuration.set(Duration.ZERO);
        minLagFromTheSourceDuration.set(Duration.ZERO);
        lastCommitDuration.set(Duration.ZERO);
        maxCommitDuration.set(Duration.ZERO);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
        committedTransactions.set(0);
        oversizedTransactions.set(0);
        registeredDmlCount.set(0);
        committedDmlCount.set(0);
        abandonedTransactionIds.set(new LRUCacheMap<>(TRANSACTION_ID_SET_SIZE));
        rolledBackTransactionIds.set(new LRUCacheMap<>(TRANSACTION_ID_SET_SIZE));
        errorCount.set(0);
        warningCount.set(0);
        scnFreezeCount.set(0);
    }

    public void setCurrentScn(Scn scn) {
        currentScn.set(scn);
    }

    public void setCurrentLogFileName(Set<String> names) {
        currentLogFileName.set(names.stream().toArray(String[]::new));
        if (names.size() < minimumLogsMined.get()) {
            minimumLogsMined.set(names.size());
        }
        else if (minimumLogsMined.get() == 0) {
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
        if (maxBatchProcessingTime.get().toMillis() < lastDuration.toMillis()) {
            maxBatchProcessingTime.set(lastDuration);
        }
        if (minBatchProcessingTime.get().toMillis() > lastDuration.toMillis()) {
            minBatchProcessingTime.set(lastDuration);
        }
        else if (minBatchProcessingTime.get().toMillis() == 0L) {
            minBatchProcessingTime.set(lastDuration);
        }
        if (getLastBatchProcessingThroughput() > maxBatchProcessingThroughput.get()) {
            maxBatchProcessingThroughput.set(getLastBatchProcessingThroughput());
        }
    }

    public void incrementNetworkConnectionProblemsCounter() {
        networkConnectionProblemsCounter.incrementAndGet();
    }

    @Override
    public BigInteger getCurrentScn() {
        return currentScn.get().asBigInteger();
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
    public int getHoursToKeepTransactionInBuffer() {
        return (int) keepTransactionsDuration.get().toHours();
    }

    @Override
    public long getMillisecondsToKeepTransactionsInBuffer() {
        return keepTransactionsDuration.get().toMillis();
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
        setLastDurationOfBatchProcessing(currentBatchProcessingTime);
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

    @Override
    public void changeBatchSize(boolean increment, boolean lobEnabled) {

        int currentBatchSize = batchSize.get();
        boolean incremented = false;
        if (increment && currentBatchSize < batchSizeMax) {
            currentBatchSize = batchSize.addAndGet(batchSizeMin);
            incremented = true;
        }
        else if (!increment && currentBatchSize > batchSizeMin) {
            currentBatchSize = batchSize.addAndGet(-batchSizeMin);
        }

        if (incremented && currentBatchSize == batchSizeMax) {
            if (!lobEnabled) {
                LOGGER.info("The connector is now using the maximum batch size {} when querying the LogMiner view. This could be indicative of large SCN gaps",
                        currentBatchSize);
            }
            else {
                LOGGER.info("The connector is now using the maximum batch size {} when querying the LogMiner view.", currentBatchSize);
            }
        }
        else {
            LOGGER.debug("Updating batch size window. Batch size {}. Min batch size {}. Max batch size {}.", currentBatchSize, batchSizeMin, batchSizeMax);
        }
    }

    // transactional buffer metrics

    @Override
    public long getNumberOfActiveTransactions() {
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
    public long getNumberOfOversizedTransactions() {
        return oversizedTransactions.get();
    }

    @Override
    public long getCommitThroughput() {
        long timeSpent = Duration.between(startTime, clock.instant()).toMillis();
        return committedTransactions.get() * MILLIS_PER_SECOND / (timeSpent != 0 ? timeSpent : 1);
    }

    @Override
    public long getRegisteredDmlCount() {
        return registeredDmlCount.get();
    }

    @Override
    public BigInteger getOldestScn() {
        return oldestScn.get().asBigInteger();
    }

    @Override
    public BigInteger getCommittedScn() {
        return committedScn.get().asBigInteger();
    }

    @Override
    public BigInteger getOffsetScn() {
        return offsetScn.get().asBigInteger();
    }

    @Override
    public long getLagFromSourceInMilliseconds() {
        return lagFromTheSourceDuration.get().toMillis();
    }

    @Override
    public long getMaxLagFromSourceInMilliseconds() {
        return maxLagFromTheSourceDuration.get().toMillis();
    }

    @Override
    public long getMinLagFromSourceInMilliseconds() {
        return minLagFromTheSourceDuration.get().toMillis();
    }

    @Override
    public Set<String> getAbandonedTransactionIds() {
        return abandonedTransactionIds.get().keySet();
    }

    @Override
    public Set<String> getRolledBackTransactionIds() {
        return rolledBackTransactionIds.get().keySet();
    }

    @Override
    public long getLastCommitDurationInMilliseconds() {
        return lastCommitDuration.get().toMillis();
    }

    @Override
    public long getMaxCommitDurationInMilliseconds() {
        return maxCommitDuration.get().toMillis();
    }

    @Override
    public int getErrorCount() {
        return errorCount.get();
    }

    @Override
    public int getWarningCount() {
        return warningCount.get();
    }

    @Override
    public int getScnFreezeCount() {
        return scnFreezeCount.get();
    }

    @Override
    public int getUnparsableDdlCount() {
        return unparsableDdlCount.get();
    }

    @Override
    public long getMiningSessionUserGlobalAreaMemoryInBytes() {
        return miningSessionUserGlobalAreaMemory.get();
    }

    @Override
    public long getMiningSessionUserGlobalAreaMaxMemoryInBytes() {
        return miningSessionUserGlobalAreaMaxMemory.get();
    }

    @Override
    public long getMiningSessionProcessGlobalAreaMemoryInBytes() {
        return miningSessionProcessGlobalAreaMemory.get();
    }

    @Override
    public long getMiningSessionProcessGlobalAreaMaxMemoryInBytes() {
        return miningSessionProcessGlobalAreaMaxMemory.get();
    }

    public void setOldestScn(Scn scn) {
        oldestScn.set(scn);
    }

    public void setCommittedScn(Scn scn) {
        committedScn.set(scn);
    }

    public void setOffsetScn(Scn scn) {
        offsetScn.set(scn);
    }

    public void setActiveTransactions(long activeTransactionCount) {
        activeTransactions.set(activeTransactionCount);
    }

    public void incrementRolledBackTransactions() {
        rolledBackTransactions.incrementAndGet();
    }

    public void incrementCommittedTransactions() {
        committedTransactions.incrementAndGet();
    }

    public void incrementOversizedTransactions() {
        oversizedTransactions.incrementAndGet();
    }

    public void incrementRegisteredDmlCount() {
        registeredDmlCount.incrementAndGet();
    }

    public void incrementCommittedDmlCount(long counter) {
        committedDmlCount.getAndAdd(counter);
    }

    public void incrementErrorCount() {
        errorCount.incrementAndGet();
    }

    public void incrementWarningCount() {
        warningCount.incrementAndGet();
    }

    public void incrementScnFreezeCount() {
        scnFreezeCount.incrementAndGet();
    }

    public void addAbandonedTransactionId(String transactionId) {
        if (transactionId != null) {
            abandonedTransactionIds.get().put(transactionId, transactionId);
        }
    }

    public void addRolledBackTransactionId(String transactionId) {
        if (transactionId != null) {
            rolledBackTransactionIds.get().put(transactionId, transactionId);
        }
    }

    public void setLastCommitDuration(Duration lastDuration) {
        lastCommitDuration.set(lastDuration);
        if (lastDuration.toMillis() > maxCommitDuration.get().toMillis()) {
            maxCommitDuration.set(lastDuration);
        }
    }

    /**
     * Calculates the time difference between the database server and the connector.
     * Along with the time difference also the offset of the database server time to UTC is stored.
     * Both values are required to calculate lag metrics.
     *
     * @param databaseSystemTime the system time (<code>SYSTIMESTAMP</code>) of the database
     */
    public void calculateTimeDifference(OffsetDateTime databaseSystemTime) {
        this.zoneOffset.set(databaseSystemTime.getOffset());
        LOGGER.trace("Timezone offset of database system time is {} seconds", zoneOffset.get().getTotalSeconds());

        Instant now = clock.instant();
        long timeDiffMillis = Duration.between(databaseSystemTime.toInstant(), now).toMillis();
        this.timeDifference.set(timeDiffMillis);
        LOGGER.trace("Current time {} ms, database difference {} ms", now.toEpochMilli(), timeDiffMillis);
    }

    public ZoneOffset getDatabaseOffset() {
        return zoneOffset.get();
    }

    public void calculateLagMetrics(Instant changeTime) {
        if (changeTime != null) {
            final Instant correctedChangeTime = changeTime.plusMillis(timeDifference.longValue()).minusSeconds(zoneOffset.get().getTotalSeconds());
            final Duration lag = Duration.between(correctedChangeTime, clock.instant()).abs();
            lagFromTheSourceDuration.set(lag);

            if (maxLagFromTheSourceDuration.get().toMillis() < lag.toMillis()) {
                maxLagFromTheSourceDuration.set(lag);
            }
            if (minLagFromTheSourceDuration.get().toMillis() > lag.toMillis()) {
                minLagFromTheSourceDuration.set(lag);
            }
            else if (minLagFromTheSourceDuration.get().toMillis() == 0) {
                minLagFromTheSourceDuration.set(lag);
            }
        }
    }

    public void incrementUnparsableDdlCount() {
        unparsableDdlCount.incrementAndGet();
    }

    public void setUserGlobalAreaMemory(long ugaMemory, long ugaMaxMemory) {
        miningSessionUserGlobalAreaMemory.set(ugaMemory);
        if (ugaMaxMemory > miningSessionUserGlobalAreaMaxMemory.get()) {
            miningSessionUserGlobalAreaMaxMemory.set(ugaMaxMemory);
        }
    }

    public void setProcessGlobalAreaMemory(long pgaMemory, long pgaMaxMemory) {
        miningSessionProcessGlobalAreaMemory.set(pgaMemory);
        if (pgaMemory > miningSessionProcessGlobalAreaMaxMemory.get()) {
            miningSessionProcessGlobalAreaMaxMemory.set(pgaMemory);
        }
    }

    @Override
    public String toString() {
        return "OracleStreamingChangeEventSourceMetrics{" +
                "currentScn=" + currentScn +
                ", oldestScn=" + oldestScn.get() +
                ", committedScn=" + committedScn.get() +
                ", offsetScn=" + offsetScn.get() +
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
                ", maxBatchProcessingThroughput=" + maxBatchProcessingThroughput +
                ", currentLogFileName=" + Arrays.asList(currentLogFileName.get()) +
                ", minLogFilesMined=" + minimumLogsMined +
                ", maxLogFilesMined=" + maximumLogsMined +
                ", redoLogStatus=" + Arrays.asList(redoLogStatus.get()) +
                ", switchCounter=" + switchCounter +
                ", batchSize=" + batchSize +
                ", millisecondToSleepBetweenMiningQuery=" + millisecondToSleepBetweenMiningQuery +
                ", keepTransactionsDuration=" + keepTransactionsDuration.get() +
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
                ", lagFromTheSource=Duration" + lagFromTheSourceDuration.get() +
                ", maxLagFromTheSourceDuration=" + maxLagFromTheSourceDuration.get() +
                ", minLagFromTheSourceDuration=" + minLagFromTheSourceDuration.get() +
                ", lastCommitDuration=" + lastCommitDuration +
                ", maxCommitDuration=" + maxCommitDuration +
                ", activeTransactions=" + activeTransactions.get() +
                ", rolledBackTransactions=" + rolledBackTransactions.get() +
                ", oversizedTransactions=" + oversizedTransactions.get() +
                ", committedTransactions=" + committedTransactions.get() +
                ", abandonedTransactionIds=" + abandonedTransactionIds.get() +
                ", rolledbackTransactionIds=" + rolledBackTransactionIds.get() +
                ", registeredDmlCount=" + registeredDmlCount.get() +
                ", committedDmlCount=" + committedDmlCount.get() +
                ", errorCount=" + errorCount.get() +
                ", warningCount=" + warningCount.get() +
                ", scnFreezeCount=" + scnFreezeCount.get() +
                ", unparsableDdlCount=" + unparsableDdlCount.get() +
                ", miningSessionUserGlobalAreaMemory=" + miningSessionUserGlobalAreaMemory.get() +
                ", miningSessionUserGlobalAreaMaxMemory=" + miningSessionUserGlobalAreaMaxMemory.get() +
                ", miningSessionProcessGlobalAreaMemory=" + miningSessionProcessGlobalAreaMemory.get() +
                ", miningSessionProcessGlobalAreaMaxMemory=" + miningSessionProcessGlobalAreaMaxMemory.get() +
                '}';
    }
}
