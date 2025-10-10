/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.LRUCacheMap;
import io.debezium.util.Strings;

/**
 * Oracle Streaming Metrics implementation for the Oracle LogMiner streaming adapter.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class LogMinerStreamingChangeEventSourceMetrics
        extends AbstractOracleStreamingChangeEventSourceMetrics
        implements LogMinerStreamingChangeEventSourceMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSourceMetrics.class);

    private static final long MILLIS_PER_SECOND = 1000L;
    private static final int TRANSACTION_ID_SET_SIZE = 10;

    private final OracleConnectorConfig connectorConfig;

    private final Instant startTime;
    private final Clock clock;

    private final AtomicReference<Scn> currentScn = new AtomicReference<>(Scn.NULL);
    private final AtomicReference<Scn> offsetScn = new AtomicReference<>(Scn.NULL);
    private final AtomicReference<Scn> commitScn = new AtomicReference<>(Scn.NULL);
    private final AtomicReference<Scn> oldestScn = new AtomicReference<>(Scn.NULL);
    private final AtomicReference<Instant> oldestScnTime = new AtomicReference<>();
    private final AtomicReference<String[]> currentLogFileNames = new AtomicReference<>(new String[0]);
    private final AtomicReference<String[]> minedLogFileNames = new AtomicReference<>(new String[0]);
    private final AtomicReference<String[]> redoLogStatuses = new AtomicReference<>(new String[0]);
    private final AtomicReference<ZoneOffset> databaseZoneOffset = new AtomicReference<>(ZoneOffset.UTC);

    private final AtomicInteger batchSize = new AtomicInteger();
    private final AtomicInteger logSwitchCount = new AtomicInteger();
    private final AtomicInteger logMinerQueryCount = new AtomicInteger();
    private final AtomicInteger jdbcRows = new AtomicInteger();

    private final AtomicLong sleepTime = new AtomicLong();
    private final AtomicLong minimumLogsMined = new AtomicLong();
    private final AtomicLong maximumLogsMined = new AtomicLong();
    private final AtomicLong maxBatchProcessingThroughput = new AtomicLong();
    private final AtomicLong timeDifference = new AtomicLong();
    private final AtomicLong processedRowsCount = new AtomicLong();
    private final AtomicLong activeTransactionCount = new AtomicLong();
    private final AtomicLong rolledBackTransactionCount = new AtomicLong();
    private final AtomicLong oversizedTransactionCount = new AtomicLong();
    private final AtomicLong changesCount = new AtomicLong();
    private final AtomicLong scnFreezeCount = new AtomicLong();
    private final AtomicLong partialRollbackCount = new AtomicLong();
    private final AtomicLong numberOfBufferedEvents = new AtomicLong();

    private final DurationHistogramMetric batchProcessingDuration = new DurationHistogramMetric();
    private final DurationHistogramMetric fetchQueryDuration = new DurationHistogramMetric();
    private final DurationHistogramMetric commitDuration = new DurationHistogramMetric();
    private final DurationHistogramMetric lagFromSourceDuration = new DurationHistogramMetric();
    private final DurationHistogramMetric miningSessionStartupDuration = new DurationHistogramMetric();
    private final DurationHistogramMetric parseTimeDuration = new DurationHistogramMetric();
    private final DurationHistogramMetric resultSetNextDuration = new DurationHistogramMetric();

    private final MaxLongValueMetric userGlobalAreaMemory = new MaxLongValueMetric();
    private final MaxLongValueMetric processGlobalAreaMemory = new MaxLongValueMetric();

    private final LRUSet<String> abandonedTransactionIds = new LRUSet<>(TRANSACTION_ID_SET_SIZE);
    private final LRUSet<String> rolledBackTransactionIds = new LRUSet<>(TRANSACTION_ID_SET_SIZE);

    public LogMinerStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext,
                                                     ChangeEventQueueMetrics changeEventQueueMetrics,
                                                     EventMetadataProvider metadataProvider,
                                                     OracleConnectorConfig connectorConfig,
                                                     CapturedTablesSupplier capturedTablesSupplier) {
        this(taskContext, changeEventQueueMetrics, metadataProvider, connectorConfig, Clock.systemUTC(), capturedTablesSupplier);
    }

    public LogMinerStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext,
                                                     ChangeEventQueueMetrics changeEventQueueMetrics,
                                                     EventMetadataProvider metadataProvider,
                                                     OracleConnectorConfig connectorConfig,
                                                     Clock clock,
                                                     CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
        this.connectorConfig = connectorConfig;
        this.batchSize.set(connectorConfig.getLogMiningBatchSizeDefault());
        this.sleepTime.set(connectorConfig.getLogMiningSleepTimeDefault().toMillis());
        this.clock = clock;
        this.startTime = clock.instant();
        this.batchMetrics = new BatchMetrics(this);
        reset();
    }

    @Override
    public void reset() {
        super.reset();

        jdbcRows.set(0);
        changesCount.set(0);
        processedRowsCount.set(0);
        logMinerQueryCount.set(0);
        activeTransactionCount.set(0);
        rolledBackTransactionCount.set(0);
        oversizedTransactionCount.set(0);
        scnFreezeCount.set(0);
        partialRollbackCount.set(0);
        numberOfBufferedEvents.set(0);

        fetchQueryDuration.reset();
        batchProcessingDuration.reset();
        parseTimeDuration.reset();
        miningSessionStartupDuration.reset();
        userGlobalAreaMemory.reset();
        processGlobalAreaMemory.reset();
        lagFromSourceDuration.reset();
        commitDuration.reset();

        abandonedTransactionIds.reset();
        rolledBackTransactionIds.reset();

        oldestScnTime.set(null);
    }

    @Override
    public long getMillisecondsToKeepTransactionsInBuffer() {
        return connectorConfig.getLogMiningTransactionRetention().toMillis();
    }

    @Override
    public long getSleepTimeInMilliseconds() {
        return sleepTime.get();
    }

    @Override
    public BigInteger getCurrentScn() {
        return currentScn.get().asBigInteger();
    }

    @Override
    public BigInteger getOffsetScn() {
        return offsetScn.get().asBigInteger();
    }

    @Override
    public BigInteger getCommittedScn() {
        return commitScn.get().asBigInteger();
    }

    @Override
    public BigInteger getOldestScn() {
        return oldestScn.get().asBigInteger();
    }

    @Override
    public long getOldestScnAgeInMilliseconds() {
        if (Objects.isNull(oldestScnTime.get())) {
            return 0L;
        }
        return Duration.between(oldestScnTime.get(), Instant.now()).abs().toMillis();
    }

    @Override
    public String[] getCurrentLogFileNames() {
        return currentLogFileNames.get();
    }

    @Override
    public String[] getMinedLogFileNames() {
        return minedLogFileNames.get();
    }

    @Override
    public int getBatchSize() {
        return batchSize.get();
    }

    @Override
    public long getMinimumMinedLogCount() {
        return minimumLogsMined.get();
    }

    @Override
    public long getMaximumMinedLogCount() {
        return maximumLogsMined.get();
    }

    @Override
    public String[] getRedoLogStatuses() {
        return redoLogStatuses.get();
    }

    @Override
    public int getLogSwitchCount() {
        return logSwitchCount.get();
    }

    @Override
    public long getTotalProcessedRows() {
        return processedRowsCount.get();
    }

    @Override
    public long getNumberOfActiveTransactions() {
        return activeTransactionCount.get();
    }

    @Override
    public long getNumberOfRolledBackTransactions() {
        return rolledBackTransactionCount.get();
    }

    @Override
    public long getNumberOfOversizedTransactions() {
        return oversizedTransactionCount.get();
    }

    @Override
    public long getTotalChangesCount() {
        return changesCount.get();
    }

    @Override
    public long getFetchQueryCount() {
        return logMinerQueryCount.get();
    }

    @Override
    public long getScnFreezeCount() {
        return scnFreezeCount.get();
    }

    @Override
    public long getLastDurationOfFetchQueryInMilliseconds() {
        return fetchQueryDuration.getLast().toMillis();
    }

    @Override
    public long getMaxDurationOfFetchQueryInMilliseconds() {
        return fetchQueryDuration.getMaximum().toMillis();
    }

    @Override
    public long getLastBatchProcessingTimeInMilliseconds() {
        return batchProcessingDuration.getLast().toMillis();
    }

    @Override
    public long getMinBatchProcessingTimeInMilliseconds() {
        return batchProcessingDuration.getMinimum().toMillis();
    }

    @Override
    public long getMaxBatchProcessingTimeInMilliseconds() {
        return batchProcessingDuration.getMaximum().toMillis();
    }

    @Override
    public long getTotalBatchProcessingTimeInMilliseconds() {
        return batchProcessingDuration.getTotal().toMillis();
    }

    @Override
    public long getCommitThroughput() {
        final long timeSpent = Duration.between(startTime, clock.instant()).toMillis();
        return getNumberOfCommittedTransactions() * MILLIS_PER_SECOND / (timeSpent != 0 ? timeSpent : 1);
    }

    @Override
    public long getLastBatchProcessingThroughput() {
        final Duration lastBatchProcessingDuration = batchProcessingDuration.getLast();
        if (lastBatchProcessingDuration.isZero()) {
            return 0L;
        }
        return Math.round(((float) jdbcRows.get() / lastBatchProcessingDuration.toMillis()) * 1000);
    }

    @Override
    public long getMaxBatchProcessingThroughput() {
        return this.maxBatchProcessingThroughput.get();
    }

    @Override
    public long getAverageBatchProcessingThroughput() {
        final Duration totalBatchProcessingDuration = batchProcessingDuration.getTotal();
        if (totalBatchProcessingDuration.isZero()) {
            return 0L;
        }
        return Math.round(((float) getTotalCapturedDmlCount() / totalBatchProcessingDuration.toMillis()) * 1000);
    }

    @Override
    public long getLastCommitDurationInMilliseconds() {
        return commitDuration.getLast().toMillis();
    }

    @Override
    public long getMaxCommitDurationInMilliseconds() {
        return commitDuration.getMaximum().toMillis();
    }

    @Override
    public long getLastMiningSessionStartTimeInMilliseconds() {
        return miningSessionStartupDuration.getLast().toMillis();
    }

    @Override
    public long getMaxMiningSessionStartTimeInMilliseconds() {
        return miningSessionStartupDuration.getMaximum().toMillis();
    }

    @Override
    public long getTotalMiningSessionStartTimeInMilliseconds() {
        return miningSessionStartupDuration.getTotal().toMillis();
    }

    @Override
    public long getTotalParseTimeInMilliseconds() {
        return parseTimeDuration.getTotal().toMillis();
    }

    @Override
    public long getTotalResultSetNextTimeInMilliseconds() {
        return resultSetNextDuration.getTotal().toMillis();
    }

    @Override
    public long getLagFromSourceInMilliseconds() {
        return lagFromSourceDuration.getLast().toMillis();
    }

    @Override
    public long getMinLagFromSourceInMilliseconds() {
        return lagFromSourceDuration.getMinimum().toMillis();
    }

    @Override
    public long getMaxLagFromSourceInMilliseconds() {
        return lagFromSourceDuration.getMaximum().toMillis();
    }

    @Override
    public long getMiningSessionUserGlobalAreaMemoryInBytes() {
        return userGlobalAreaMemory.getValue();
    }

    @Override
    public long getMiningSessionUserGlobalAreaMaxMemoryInBytes() {
        return userGlobalAreaMemory.getMax();
    }

    @Override
    public long getMiningSessionProcessGlobalAreaMemoryInBytes() {
        return processGlobalAreaMemory.getValue();
    }

    @Override
    public long getMiningSessionProcessGlobalAreaMaxMemoryInBytes() {
        return processGlobalAreaMemory.getMax();
    }

    @Override
    public Set<String> getAbandonedTransactionIds() {
        return abandonedTransactionIds.getAll();
    }

    @Override
    public long getAbandonedTransactionCount() {
        return abandonedTransactionIds.getAll().size();
    }

    @Override
    public long getNumberOfPartialRollbackCount() {
        return partialRollbackCount.get();
    }

    @Override
    public Set<String> getRolledBackTransactionIds() {
        return rolledBackTransactionIds.getAll();
    }

    @Override
    public long getNumberOfEventsInBuffer() {
        return numberOfBufferedEvents.get();
    }

    /**
     * @return database current zone offset
     */
    public ZoneOffset getDatabaseOffset() {
        return databaseZoneOffset.get();
    }

    /**
     * Set the currently used batch size for querying LogMiner.
     *
     * @param batchSize batch size used for querying LogMiner
     */
    public void setBatchSize(int batchSize) {
        this.batchSize.set(batchSize);
    }

    /**
     * Set the connector's currently used sleep/pause time between LogMiner queries.
     *
     * @param sleepTime sleep time between LogMiner queries
     */
    public void setSleepTime(long sleepTime) {
        this.sleepTime.set(sleepTime);
    }

    /**
     * Set the current system change number from the database.
     *
     * @param currentScn database current system change number
     */
    public void setCurrentScn(Scn currentScn) {
        this.currentScn.set(currentScn);
    }

    /**
     * Set the offset's low-watermark system change number.
     *
     * @param offsetScn offset's restart system change number, i.e. {@code scn} attribute
     */
    public void setOffsetScn(Scn offsetScn) {
        this.offsetScn.set(offsetScn);
    }

    /**
     * Sets the offset's high-watermark system change number.
     *
     * @param commitScn offset's commit system change number, i.e. {@code commit_scn} attribute
     */
    // todo: getter is Committed, should this match?
    public void setCommitScn(Scn commitScn) {
        this.commitScn.set(commitScn);
    }

    /**
     * Sets the details for the oldest system change number in the transaction buffer.
     *
     * @param oldestScn oldest system change number
     * @param changeTime time when the oldest system change number was created
     */
    public void setOldestScnDetails(Scn oldestScn, Instant changeTime) {
        this.oldestScn.set(oldestScn);
        this.oldestScnTime.set(oldestScn.isNull() ? null : getAdjustedChangeTime(changeTime));
    }

    /**
     * Set the current iteration's online redo logs that are being mined.
     *
     * @param redoLogFileNames current set of online redo logs that are part of the mining session.
     */
    public void setCurrentLogFileNames(Set<String> redoLogFileNames) {
        this.currentLogFileNames.set(redoLogFileNames.toArray(String[]::new));
    }

    /**
     * Set all logs that are currently being mined.
     *
     * @param minedLogFileNames all logs that are part of the mining session
     */
    public void setMinedLogFileNames(Set<String> minedLogFileNames) {
        this.minedLogFileNames.set(minedLogFileNames.toArray(String[]::new));
        if (minedLogFileNames.size() < minimumLogsMined.get()) {
            minimumLogsMined.set(minedLogFileNames.size());
        }
        else if (minimumLogsMined.get() == 0) {
            minimumLogsMined.set(minedLogFileNames.size());
        }
        if (minedLogFileNames.size() > maximumLogsMined.get()) {
            maximumLogsMined.set(minedLogFileNames.size());
        }
    }

    /**
     * Set the current logs and their respective statuses.
     *
     * @param statuses map of file names as key and the file's status as the value.
     */
    public void setRedoLogStatuses(Map<String, String> statuses) {
        redoLogStatuses.set(statuses.entrySet().stream()
                .map(entry -> entry.getKey() + " | " + entry.getValue())
                .toArray(String[]::new));
    }

    /**
     * Set the number of log switches in the past day.
     *
     * @param logSwitchCount number of log switches
     */
    public void setSwitchCount(int logSwitchCount) {
        this.logSwitchCount.set(logSwitchCount);
    }

    /**
     * Sets the last number of rows processed by the LogMiner mining iteration.
     *
     * @param processedRowsCount number of rows processed in the last mining iteration
     */
    public void setLastProcessedRowsCount(long processedRowsCount) {
        this.processedRowsCount.getAndAdd(processedRowsCount);
    }

    /**
     * Sets the number of current, active transactions in the transaction buffer.
     *
     * @param activeTransactionCount number of active transactions
     */
    public void setActiveTransactionCount(long activeTransactionCount) {
        this.activeTransactionCount.set(activeTransactionCount);
    }

    /**
     * Sets the number of transaction events in the buffer.
     *
     * @param bufferedEventCount number of buffered events
     */
    public void setBufferedEventCount(long bufferedEventCount) {
        this.numberOfBufferedEvents.set(bufferedEventCount);
    }

    /**
     * Increments the number of rolled back transactions.
     */
    public void incrementRolledBackTransactionCount() {
        rolledBackTransactionCount.incrementAndGet();
    }

    /**
     * Increments the number of over-sized transactions.
     */
    public void incrementOversizedTransactionCount() {
        oversizedTransactionCount.incrementAndGet();
    }

    /**
     * Increments the total changes seen.
     */
    public void incrementTotalChangesCount() {
        changesCount.incrementAndGet();
    }

    /**
     * Increments the number of LogMiner queries executed.
     */
    public void incrementLogMinerQueryCount() {
        logMinerQueryCount.incrementAndGet();
    }

    /**
     * Increments the number of times the system change number is considered frozen and has not
     * changed over several consecutive LogMiner query batches.
     */
    public void incrementScnFreezeCount() {
        scnFreezeCount.incrementAndGet();
    }

    /**
     * Sets the number of times the system change number is considered frozen and has not changed over several consecutive
     * LogMiner query batches.
     *
     * @param scnFreezeCount number of times the system change number is considered frozen
     */
    public void setScnFreezeCount(long scnFreezeCount) {
        this.scnFreezeCount.set(scnFreezeCount);
    }

    /**
     * Sets the duration of the last LogMiner query execution.
     *
     * @param duration duration of the last LogMiner query
     */
    public void setLastDurationOfFetchQuery(Duration duration) {
        fetchQueryDuration.set(duration);
        logMinerQueryCount.incrementAndGet();
    }

    /**
     * Sets the duration of the total processing of the last LogMiner query result-set.
     *
     * @param duration duration of the total processing of the last LogMiner query result-set
     */
    public void setLastBatchProcessingDuration(Duration duration) {
        batchProcessingDuration.set(duration);

        final long lastBatchProcessingThroughput = getLastBatchProcessingThroughput();
        if (lastBatchProcessingThroughput > getMaxBatchProcessingThroughput()) {
            maxBatchProcessingThroughput.set(lastBatchProcessingThroughput);
        }
    }

    /**
     * Sets last number of JDBC rows read from Oracle in the last LogMiner query result-set.
     *
     * @param jdbcRows the number of JDBC rows read in the last LogMiner query result-set
     */
    public void setLastBatchJdbcRows(int jdbcRows) {
        this.jdbcRows.set(jdbcRows);
    }

    /**
     * Sets the duration of the last transaction commit processing.
     *
     * @param duration duration of the last transaction commit processing
     */
    public void setLastCommitDuration(Duration duration) {
        commitDuration.set(duration);
    }

    /**
     * Sets the duration of the last LogMiner mining session start-up and data dictionary load.
     *
     * @param duration duration of the last LogMiner session start-up
     */
    public void setLastMiningSessionStartDuration(Duration duration) {
        miningSessionStartupDuration.set(duration);
    }

    /**
     * Sets the duration for parsing the last SQL statement.
     *
     * @param duration duration for parsing the last SQL statement
     */
    public void setLastParseTimeDuration(Duration duration) {
        parseTimeDuration.set(duration);
    }

    /**
     * Sets the duration for the last {@code ResultSet#next} function call.
     *
     * @param duration duration for the last result set next call
     */
    public void setLastResultSetNextDuration(Duration duration) {
        resultSetNextDuration.set(duration);
    }

    /**
     * Set the database's current user global area (UGA) memory statistics.
     *
     * @param memory current user global area memory
     * @param maxMemory maximum user global area memory
     */
    public void setUserGlobalAreaMemory(long memory, long maxMemory) {
        userGlobalAreaMemory.setValue(memory);
        userGlobalAreaMemory.setMax(maxMemory);
    }

    /**
     * Set the database's current process global area (PGA) memory statistics.
     *
     * @param memory current process global area memory
     * @param maxMemory maximum process global area memory
     */
    public void setProcessGlobalAreaMemory(long memory, long maxMemory) {
        processGlobalAreaMemory.setValue(memory);
        processGlobalAreaMemory.setMax(maxMemory);
    }

    /**
     * Add a transaction to the recently tracked abandoned transactions metric.
     *
     * @param transactionId transaction identifier
     */
    public void addAbandonedTransactionId(String transactionId) {
        if (!Strings.isNullOrBlank(transactionId)) {
            abandonedTransactionIds.add(transactionId);
        }
    }

    /**
     * Add a transaction to the recently rolled back transactions metric.
     *
     * @param transactionId transaction identifier
     */
    public void addRolledBackTransactionId(String transactionId) {
        if (!Strings.isNullOrBlank(transactionId)) {
            rolledBackTransactionIds.add(transactionId);
        }
    }

    /**
     * Sets the database time zone and calculates the difference in time between the database server
     * and the connector. These values are necessary to calculate lag metrics.
     *
     * @param databaseSystemTime the database {@code SYSTIMESTAMP} value
     */
    public void setDatabaseTimeDifference(OffsetDateTime databaseSystemTime) {
        this.databaseZoneOffset.set(databaseSystemTime.getOffset());
        LOGGER.trace("Timezone offset of database time is {} seconds.", databaseZoneOffset.get().getTotalSeconds());

        final Instant now = clock.instant();
        final long timeDifferenceInMilliseconds = Duration.between(databaseSystemTime.toInstant(), now).toMillis();
        this.timeDifference.set(timeDifferenceInMilliseconds);
        LOGGER.trace("Current time {} ms, database difference {} ms", now.toEpochMilli(), timeDifferenceInMilliseconds);
    }

    /**
     * Calculates the lag metrics based on the provided event's database change time.
     *
     * @param changeTime the change time when the database recorded the event
     */
    public void calculateLagFromSource(Instant changeTime) {
        if (changeTime != null) {
            lagFromSourceDuration.set(Duration.between(getAdjustedChangeTime(changeTime), clock.instant()).abs());
        }
    }

    /**
     * Increases the number of partial rollback events detected.
     */
    public void increasePartialRollbackCount() {
        partialRollbackCount.addAndGet(1);
    }

    @Override
    public String toString() {
        return "LogMinerStreamingChangeEventSourceMetrics{" +
                "connectorConfig=" + connectorConfig +
                ", startTime=" + startTime +
                ", clock=" + clock +
                ", currentScn=" + currentScn +
                ", offsetScn=" + offsetScn +
                ", commitScn=" + commitScn +
                ", oldestScn=" + oldestScn +
                ", oldestScnTime=" + oldestScnTime +
                ", currentLogFileNames=" + Arrays.asList(currentLogFileNames.get()) +
                ", redoLogStatuses=" + Arrays.asList(redoLogStatuses.get()) +
                ", databaseZoneOffset=" + databaseZoneOffset +
                ", batchSize=" + batchSize +
                ", logSwitchCount=" + logSwitchCount +
                ", logMinerQueryCount=" + logMinerQueryCount +
                ", sleepTime=" + sleepTime +
                ", minimumLogsMined=" + minimumLogsMined +
                ", maximumLogsMined=" + maximumLogsMined +
                ", maxBatchProcessingThroughput=" + maxBatchProcessingThroughput +
                ", timeDifference=" + timeDifference +
                ", processedRowsCount=" + processedRowsCount +
                ", activeTransactionCount=" + activeTransactionCount +
                ", rolledBackTransactionCount=" + rolledBackTransactionCount +
                ", oversizedTransactionCount=" + oversizedTransactionCount +
                ", changesCount=" + changesCount +
                ", scnFreezeCount=" + scnFreezeCount +
                ", batchProcessingDuration=" + batchProcessingDuration +
                ", fetchQueryDuration=" + fetchQueryDuration +
                ", commitDuration=" + commitDuration +
                ", lagFromSourceDuration=" + lagFromSourceDuration +
                ", miningSessionStartupDuration=" + miningSessionStartupDuration +
                ", parseTimeDuration=" + parseTimeDuration +
                ", resultSetNextDuration=" + resultSetNextDuration +
                ", userGlobalAreaMemory=" + userGlobalAreaMemory +
                ", processGlobalAreaMemory=" + processGlobalAreaMemory +
                ", abandonedTransactionIds=" + abandonedTransactionIds +
                ", rolledBackTransactionIds=" + rolledBackTransactionIds +
                "} ";
    }

    private Instant getAdjustedChangeTime(Instant changeTime) {
        if (changeTime == null) {
            return null;
        }
        return changeTime.plusMillis(timeDifference.longValue())
                .minusSeconds(databaseZoneOffset.get().getTotalSeconds());
    }

    /**
     * Utility class for tracking histogram-based values for a duration-based metric.
     */
    @ThreadSafe
    static class DurationHistogramMetric {

        private final AtomicReference<Duration> min = new AtomicReference<>(Duration.ZERO);
        private final AtomicReference<Duration> max = new AtomicReference<>(Duration.ZERO);
        private final AtomicReference<Duration> last = new AtomicReference<>(Duration.ZERO);
        private final AtomicReference<Duration> total = new AtomicReference<>(Duration.ZERO);

        /**
         * Resets the duration metric
         */
        void reset() {
            min.set(Duration.ZERO);
            max.set(Duration.ZERO);
            last.set(Duration.ZERO);
            total.set(Duration.ZERO);
        }

        /**
         * Sets the last duration-based value for the histogram.
         *
         * @param lastDuration last duration
         */
        void set(Duration lastDuration) {
            last.set(lastDuration);
            total.accumulateAndGet(lastDuration, Duration::plus);
            if (max.get().toMillis() < lastDuration.toMillis()) {
                max.set(lastDuration);
            }
            final long minimumValue = min.get().toMillis();
            if (minimumValue > lastDuration.toMillis()) {
                min.set(lastDuration);
            }
            else if (minimumValue == 0L) {
                min.set(lastDuration);
            }
        }

        Duration getMinimum() {
            return min.get();
        }

        Duration getMaximum() {
            return max.get();
        }

        Duration getLast() {
            return last.get();
        }

        Duration getTotal() {
            return total.get();
        }

        @Override
        public String toString() {
            return String.format("{min=%s,max=%s,total=%s}", min.get(), max.get(), total.get());
        }
    }

    /**
     * Utility class for tracking the current and maximum long value.
     */
    @ThreadSafe
    static class MaxLongValueMetric {

        private final AtomicLong value = new AtomicLong();
        private final AtomicLong max = new AtomicLong();

        public void reset() {
            value.set(0L);
            max.set(0L);
        }

        public void setValueAndCalculateMax(long value) {
            this.value.set(value);
            if (max.get() < value) {
                max.set(value);
            }
        }

        public void setValue(long value) {
            this.value.set(value);
        }

        public void setMax(long max) {
            if (this.max.get() < max) {
                this.max.set(max);
            }
        }

        public long getValue() {
            return value.get();
        }

        public long getMax() {
            return max.get();
        }

        @Override
        public String toString() {
            return String.format("{value=%d,max=%d}", value.get(), max.get());
        }
    }

    /**
     * Utility class for maintaining a least-recently-used list of values.
     *
     * @param <T> the argument type to be stored
     */
    @ThreadSafe
    static class LRUSet<T> {

        private final AtomicReference<LRUCacheMap<T, T>> cache = new AtomicReference<>();
        private final int capacity;

        LRUSet(int capacity) {
            this.cache.set(new LRUCacheMap<>(capacity));
            this.capacity = capacity;
        }

        public void reset() {
            this.cache.set(new LRUCacheMap<>(capacity));
        }

        public void add(T value) {
            this.cache.get().put(value, value);
        }

        public Set<T> getAll() {
            return this.cache.get().keySet();
        }

        @Override
        public String toString() {
            return getAll().toString();
        }
    }

    private final BatchMetrics batchMetrics;

    public BatchMetrics getBatchMetrics() {
        return batchMetrics;
    }

    public static class BatchMetrics {

        private final LogMinerStreamingChangeEventSourceMetrics metrics;

        private int schemaChangeCount;
        private int dataChangeCount;
        private int insertCount;
        private int updateCount;
        private int deleteCount;
        private int commitCount;
        private int rollbackCount;
        private int partialRollbackCount;
        private int jdbcRows;
        private int processedRows;
        private int metadataQueryCount;

        public BatchMetrics(LogMinerStreamingChangeEventSourceMetrics metrics) {
            this.metrics = metrics;
        }

        public void schemaChangeObserved() {
            schemaChangeCount++;
        }

        public void dataChangeEventObserved(EventType eventType) {
            dataChangeCount++;

            switch (eventType) {
                case INSERT -> insertCount++;
                case UPDATE -> updateCount++;
                case DELETE -> deleteCount++;
            }
        }

        public void commitObserved() {
            commitCount++;
        }

        public void rollbackObserved() {
            rollbackCount++;
        }

        public void partialRollbackObserved() {
            partialRollbackCount++;
        }

        public void rowObserved() {
            jdbcRows++;
        }

        public void rowProcessed() {
            processedRows++;
        }

        public void tableMetadataQueryObserved() {
            metadataQueryCount++;
        }

        public void updateStreamingMetrics() {
            metrics.setLastCapturedDmlCount(dataChangeCount);
            metrics.setLastProcessedRowsCount(processedRows);
            metrics.setLastBatchJdbcRows(jdbcRows);
        }

        public boolean hasProcessedAnyTransactions() {
            return dataChangeCount > 0 || commitCount > 0 || rollbackCount > 0;
        }

        public boolean hasJdbcRows() {
            return jdbcRows > 0;
        }

        public void reset() {
            schemaChangeCount = 0;
            dataChangeCount = 0;
            insertCount = 0;
            updateCount = 0;
            deleteCount = 0;
            commitCount = 0;
            rollbackCount = 0;
            partialRollbackCount = 0;
            jdbcRows = 0;
            processedRows = 0;
            metadataQueryCount = 0;
        }

        @Override
        public String toString() {
            return "BatchMetrics: " +
                    "jdbcRows=" + jdbcRows +
                    ", processedRows=" + processedRows +
                    ", dmlCount=" + dataChangeCount +
                    ", ddlCount=" + schemaChangeCount +
                    ", insertCount=" + insertCount +
                    ", updateCount=" + updateCount +
                    ", deleteCount=" + deleteCount +
                    ", commitCount=" + commitCount +
                    ", rollbackCount=" + rollbackCount +
                    ", partialRollbackCount=" + partialRollbackCount +
                    ", tableMetadataCount=" + metadataQueryCount;
        }
    }
}
