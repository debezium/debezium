/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Set;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * The JMX exposed interface for Oracle streaming metrics.
 */
public interface OracleStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * @return the current system change number of the database
     */
    String getCurrentScn();

    /**
     * @return array of current filenames to be used by the mining session.
     */
    String[] getCurrentRedoLogFileName();

    /**
     * @return the minimum number of logs used by a mining session
     */
    long getMinimumMinedLogCount();

    /**
     * @return the maximum number of logs used by a mining session
     */
    long getMaximumMinedLogCount();

    /**
     * Exposes states of redo logs: current, active, inactive, unused ...
     * @return array of: (redo log name | status) elements
     */
    String[] getRedoLogStatus();

    /**
     * fetches counter of redo switches for the last day.
     * If this number is high , like once in 3 minutes,  the troubleshooting on the database level is required.
     * @return counter
     */
    int getSwitchCounter();

    /**
     * @return number of captured DML since the connector is up
     */
    long getTotalCapturedDmlCount();

    /**
     * @return average duration of LogMiner view query
     */
    Long getMaxDurationOfFetchQueryInMilliseconds();

    /**
     * LogMiner view query returns number of captured DML , Commit and Rollback. This is what we call a batch.
     * @return duration of the last batch fetching
     */
    Long getLastDurationOfFetchQueryInMilliseconds();

    /**
     * LogMiner view query returns number of captured DML , Commit and Rollback. This is what we call a batch.
     * @return number of all processed batches
     */
    long getFetchingQueryCount();

    /**
     * @return max number of DML captured during connector start time
     */
    Long getMaxCapturedDmlInBatch();

    /**
     * @return time of processing the last captured batch
     */
    long getLastBatchProcessingTimeInMilliseconds();

    /**
     * @return number of captured DL during last mining session
     */
    int getLastCapturedDmlCount();

    /**
     * Maximum number of entries in LogMiner view to fetch. This is used to set the diapason of the SCN in mining query.
     * If difference between "start SCN" and "end SCN" to mine exceeds this limit, end SCN will be set to "start SCN" + batchSize
     * @return the limit
     */
    int getBatchSize();

    /**
     * this gives ability to manipulate number of entries in LogMiner view to fetch.
     * It has limits to prevent abnormal values
     * @param size limit
     */
    void setBatchSize(int size);

    /**
     * @return number of milliseconds for connector to sleep before fetching another batch from the LogMiner view
     */
    long getMillisecondToSleepBetweenMiningQuery();

    /**
     * sets number of milliseconds for connector to sleep before fetching another batch from the LogMiner view
     * @param milliseconds to sleep
     */
    void setMillisecondToSleepBetweenMiningQuery(long milliseconds);

    /**
     * change sleeping time
     * @param increment true to add, false to deduct
     */
    void changeSleepingTime(boolean increment);

    void changeBatchSize(boolean increment, boolean lobEnabled);

    /**
     * This represents the maximum number of entries processed per second from LogMiner sessions.
     * Entries include things such as DMLs, commits, rollbacks, etc.
     *
     * @return the maximum number of entries processed per second from LogMiner sessions
     */
    long getMaxBatchProcessingThroughput();

    /**
     * This represents the average number of entries processed per second from LogMiner sessions.
     * Entries include things such as DMLs, commits, rollbacks, etc.
     *
     * @return the average number of entries per second from LogMiner sessions
     */
    long getAverageBatchProcessingThroughput();

    /**
     * This represents the number of entries processed per second in the last LogMiner session.
     * Entries include things such as DMLs, commits, rollbacks, etc.
     *
     * @return the number of entries processed per second from last LogMiner session
     */
    long getLastBatchProcessingThroughput();

    /**
     * @return the number of connection problems detected
     */
    long getNetworkConnectionProblemsCounter();

    /**
     * @return the total number of milliseconds used to parse DDL/DML statements
     */
    long getTotalParseTimeInMilliseconds();

    /**
     * @return the total number of milliseconds spent starting a log mining session
     */
    long getTotalMiningSessionStartTimeInMilliseconds();

    /**
     * @return the total number of milliseconds the last mining session took to start
     */
    long getLastMiningSessionStartTimeInMilliseconds();

    /**
     * @return the duration in milliseconds of the longest mining session start
     */
    long getMaxMiningSessionStartTimeInMilliseconds();

    /**
     * @return the total number of milliseconds spent mining and processing results
     */
    long getTotalProcessingTimeInMilliseconds();

    /**
     * @return the minimum time in milliseconds spent processing results from a single LogMiner session
     */
    long getMinBatchProcessingTimeInMilliseconds();

    /**
     * @return the maximum time in milliseconds spent processing results from a single LogMiner session
     */
    long getMaxBatchProcessingTimeInMilliseconds();

    /**
     * @return the total number of log miner rows processed.
     */
    long getTotalProcessedRows();

    /**
     * @return the total number of milliseconds spent iterating log miner results calling next.
     */
    long getTotalResultSetNextTimeInMilliseconds();

    /**
     * @return the number of hours to keep transaction in buffer before abandoning
     *
     * @deprecated Use {@link #getMillisecondsToKeepTransactionsInBuffer()} instead.
     */
    @Deprecated
    int getHoursToKeepTransactionInBuffer();

    /**
     * @return the number of milliseconds to keep transactions in the buffer before abandoning
     */
    long getMillisecondsToKeepTransactionsInBuffer();

    /**
     * @return number of current active transactions in the transaction buffer
     */
    long getNumberOfActiveTransactions();

    /**
     * @return the number of committed transactions in the transaction buffer
     */
    long getNumberOfCommittedTransactions();

    /**
     * @return the number of rolled back transactions in the transaction buffer
     */
    long getNumberOfRolledBackTransactions();

    /**
     * @return the number of abandoned transactions because of number of events oversized
     */
    long getNumberOfOversizedTransactions();

    /**
     * @return average number of committed transactions per second in the transaction buffer
     */
    long getCommitThroughput();

    /**
     * @return the number of registered DML operations in the transaction buffer
     */
    long getRegisteredDmlCount();

    /**
     * @return the oldest SCN in the transaction buffer
     */
    String getOldestScn();

    /**
     * @return the last committed SCN from the transaction buffer
     */
    String getCommittedScn();

    /**
     * @return the current offset SCN
     */
    String getOffsetScn();

    /**
     * Lag can temporarily be inaccurate on DST changes.
     * This is because the timestamps received from LogMiner are in the database local time and do not contain time zone information.
     *
     * @return lag in milliseconds of latest captured change timestamp from transaction logs and it's placement in the transaction buffer.
     */
    long getLagFromSourceInMilliseconds();

    /**
     * @return maximum lag in milliseconds with the data source
     */
    long getMaxLagFromSourceInMilliseconds();

    /**
     * @return minimum lag in milliseconds with the data source
     */
    long getMinLagFromSourceInMilliseconds();

    /**
     * @return list of abandoned transaction ids from the transaction buffer
     */
    Set<String> getAbandonedTransactionIds();

    /**
     * @return slist of rolled back transaction ids from the transaction buffer
     */
    Set<String> getRolledBackTransactionIds();

    /**
     * @return total duration in milliseconds the last commit operation took in the transaction buffer
     */
    long getLastCommitDurationInMilliseconds();

    /**
     * @return the duration in milliseconds that the longest commit operation took in the transaction buffer
     */
    long getMaxCommitDurationInMilliseconds();

    /**
     * @return the number of errors detected in the connector's log
     */
    int getErrorCount();

    /**
     * @return the number of warnings detected in the connector's log
     */
    int getWarningCount();

    /**
     * @return the number of number of times the SCN does not change and is considered frozen
     */
    int getScnFreezeCount();

    /**
     * @return the number of unparsable ddl statements
     */
    int getUnparsableDdlCount();

    /**
     * @return the current mining session's UGA memory usage in bytes.
     */
    long getMiningSessionUserGlobalAreaMemoryInBytes();

    /**
     * @return the current mining session's UGA maximum memory usage in bytes.
     */
    long getMiningSessionUserGlobalAreaMaxMemoryInBytes();

    /**
     * @return the current mining session's PGA memory usage in bytes.
     */
    long getMiningSessionProcessGlobalAreaMemoryInBytes();

    /**
     * @return the current mining session's PGA maximum memory usage in bytes.
     */
    long getMiningSessionProcessGlobalAreaMaxMemoryInBytes();

    /**
     * Resets metrics.
     */
    void reset();
}
