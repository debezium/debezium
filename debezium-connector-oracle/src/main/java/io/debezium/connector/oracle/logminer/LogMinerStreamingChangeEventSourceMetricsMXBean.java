/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Set;

import io.debezium.connector.oracle.OracleCommonStreamingChangeEventSourceMetricsMXBean;

/**
 * Oracle Streaming Metrics for Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public interface LogMinerStreamingChangeEventSourceMetricsMXBean
        extends OracleCommonStreamingChangeEventSourceMetricsMXBean {

    /**
     * @return array of currently mined log files
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getCurrentLogFileNames()}
     */
    @Deprecated
    default String[] getCurrentRedoLogFileName() {
        return getCurrentLogFileNames();
    }

    /**
     * @return array of log files and their respective statues in Oracle
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getRedoLogStatuses()}
     */
    @Deprecated
    default String[] getRedoLogStatus() {
        return getRedoLogStatuses();
    }

    /**
     * @return number of log switches observed in the last day
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getLogSwitchCount()}
     */
    @Deprecated
    default int getSwitchCounter() {
        return getLogSwitchCount();
    }

    /**
     * @return number of LogMiner queries executed by the connector
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getFetchQueryCount()}
     */
    @Deprecated
    default long getFetchingQueryCount() {
        return getFetchQueryCount();
    }

    /**
     * @return duration in hours that transactions are retained in the transaction buffer
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getMillisecondsToKeepTransactionsInBuffer()}
     */
    @Deprecated
    default int getHoursToKeepTransactionInBuffer() {
        return (int) Duration.ofMillis(getMillisecondsToKeepTransactionsInBuffer()).toHours();
    }

    /**
     * @return total duration in milliseconds for processing all LogMiner query batches
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getTotalBatchProcessingTimeInMilliseconds()}
     */
    @Deprecated
    default long getTotalProcessingTimeInMilliseconds() {
        return getTotalBatchProcessingTimeInMilliseconds();
    }

    /**
     * @return total number of change events
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getTotalChangesCount()}
     */
    @Deprecated
    default long getRegisteredDmlCount() {
        return getTotalChangesCount();
    }

    /**
     * @return number of milliseconds to sleep between each LogMiner query
     * @deprecated to be removed in Debezium 2.7, replaced by {@link #getSleepTimeInMilliseconds()}
     */
    @Deprecated
    default long getMillisecondsToSleepBetweenMiningQuery() {
        return getSleepTimeInMilliseconds();
    }

    /**
     * @return total number of network problems
     * @deprecated to be removed in Debezium 2.7 with no replacement
     */
    @Deprecated
    default long getNetworkConnectionProblemsCounter() {
        // This was never used except in tests
        return 0L;
    }

    /**
     * Specifies the number of milliseconds that transactions are retained in the transaction buffer
     * before the connector discards them due to their age. When set to {@code 0}, transactions are
     * retained until they are either committed or rolled back.
     *
     * @return number of milliseconds that transactions are buffered before being discarded
     */
    long getMillisecondsToKeepTransactionsInBuffer();

    /**
     * @return number of milliseconds that the connector sleeps between LogMiner queries
     */
    long getSleepTimeInMilliseconds();

    /**
     * @return the current system change number of the database
     */
    BigInteger getCurrentScn();

    /**
     * Oracle maintains two watermarks, a low and high system change number watermark. The low
     * watermark is the offset system change number, which represents the position in the logs
     * where the connector will begin reading changes upon restart.
     *
     * @return the system change number where the connector will start from on restarts
     */
    BigInteger getOffsetScn();

    /**
     * Oracle maintains two watermarks, a low and high system change number watermark. The high
     * watermark is the commit system change number, which represents the position in the logs
     * where the last transaction commit occurred. This system change number is used to avoid
     * dispatching any transaction that committed before this system change number.
     *
     * @return the system change number where the connector last observed a commit
     */
    BigInteger getCommittedScn();

    /**
     * @return oldest system change number currently in the transaction buffer
     */
    BigInteger getOldestScn();

    /**
     * @return age in milliseconds of the oldest system change number in the transaction buffer
     */
    long getOldestScnAgeInMilliseconds();

    /**
     * @return array of online redo log filenames to be used by the mining session
     */
    String[] getCurrentLogFileNames();

    /**
     * @return array of all archive and redo logs that are read by the mining session
     */
    String[] getMinedLogFileNames();

    /**
     * Specifies the maximum gap between the start and end system change number range used for
     * querying changes from LogMiner.
     *
     * @return the LogMiner query batch size
     */
    int getBatchSize();

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
    String[] getRedoLogStatuses();

    /**
     * @return the number of redo log switches for the current day
     */
    int getLogSwitchCount();

    /**
     * @return the total number of database rows processed from LogMiner
     */
    long getTotalProcessedRows();

    /**
     * @return number of current active transactions in the transaction buffer
     */
    long getNumberOfActiveTransactions();

    /**
     * @return number of transactions seen that were rolled back
     */
    long getNumberOfRolledBackTransactions();

    /**
     * @return number of discarded transactions due to exceeding max event size
     */
    long getNumberOfOversizedTransactions();

    /**
     * @return total number of changes seen by the connector.
     */
    long getTotalChangesCount();

    /**
     * @return number of LogMiner queries executed.
     */
    long getFetchQueryCount();

    /**
     * @return number of times the system change number does not change over consecutive LogMiner queries
     */
    long getScnFreezeCount();

    /**
     * @return duration of the last LogMiner query execution in milliseconds
     */
    long getLastDurationOfFetchQueryInMilliseconds();

    /**
     * @return maximum duration across all LogMiner queries executed in milliseconds
     */
    long getMaxDurationOfFetchQueryInMilliseconds();

    /**
     * @return duration for processing the results of the last LogMiner query in milliseconds
     */
    long getLastBatchProcessingTimeInMilliseconds();

    /**
     * @return minimum duration in milliseconds for processing results from a LogMiner query
     */
    long getMinBatchProcessingTimeInMilliseconds();

    /**
     * @return maximum duration in milliseconds for processing results from a LogMiner query
     */
    long getMaxBatchProcessingTimeInMilliseconds();

    /**
     * @return total duration in milliseconds for processing results for all LogMiner queries
     */
    long getTotalBatchProcessingTimeInMilliseconds();

    /**
     * @return average number of committed transactions per second in the transaction buffer
     */
    long getCommitThroughput();

    /**
     * @return throughput per second for last LogMiner session
     */
    long getLastBatchProcessingThroughput();

    /**
     * @return maximum throughput per second across all LogMiner sessions
     */
    long getMaxBatchProcessingThroughput();

    /**
     * @return average throughput per second across all LogMiner sessions
     */
    long getAverageBatchProcessingThroughput();

    /**
     * @return duration for processing the last transaction commit in milliseconds
     */
    long getLastCommitDurationInMilliseconds();

    /**
     * @return maximum duration for processing a transaction commit in milliseconds
     */
    long getMaxCommitDurationInMilliseconds();

    /**
     * @return duration in milliseconds for the last LogMiner session start-up and data dictionary load
     */
    long getLastMiningSessionStartTimeInMilliseconds();

    /**
     * @return maximum duration in milliseconds for a LogMiner session start-up and data dictionary load
     */
    long getMaxMiningSessionStartTimeInMilliseconds();

    /**
     * @return total duration in milliseconds for all LogMiner session start-ups and data dictionary loads
     */
    long getTotalMiningSessionStartTimeInMilliseconds();

    /**
     * @return total duration in milliseconds for parsing SQL statements
     */
    long getTotalParseTimeInMilliseconds();

    /**
     * Each time a row is processed, the connector makes a call to the underlying JDBC driver to
     * fetch the next row and sometimes this fetch may need to make a round-trip to the database
     * to get the next batch of rows. This metric tracks the total time spent in milliseconds
     * for this particular call over the lifetime of the connector.
     *
     * @return total duration in milliseconds for all {@code ResultSet#next} calls
     */
    long getTotalResultSetNextTimeInMilliseconds();

    /**
     * Returns the time in milliseconds between when the database captured the change and when the
     * change is placed into the transaction buffer by the connector.
     *
     * @return duration of the lag from the source database in milliseconds
     */
    long getLagFromSourceInMilliseconds();

    /**
     * Returns the minimum time difference in milliseconds between the database capture time and
     * the time when the event is placed into the transaction buffer by the connector.
     *
     * @return minimum duration of the lag from the source database in milliseconds
     */
    long getMinLagFromSourceInMilliseconds();

    /**
     * Returns the maximum time difference in milliseconds between the database capture time and
     * the time when the event is placed into the transaction buffer by the connector.
     *
     * @return maximum duration of the lag from the source database in milliseconds
     */
    long getMaxLagFromSourceInMilliseconds();

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
     * @return most recent transaction identifiers that were abandoned
     */
    Set<String> getAbandonedTransactionIds();

    long getAbandonedTransactionCount();

    /**
     * @return the number of events that were partially rolled back in committed transactions
     */
    long getNumberOfPartialRollbackCount();

    /**
     * @return most recent transaction identifiers that were rolled back
     */
    Set<String> getRolledBackTransactionIds();

}
