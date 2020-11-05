/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

/**
 * This interface is exposed for JMX
 */
public interface LogMinerMetricsMXBean {

    /**
     * Exposes current SCN in the database. This is very efficient query and will not affect overall performance
     *
     * @return current SCN
     */
    Long getCurrentScn();

    /**
     * Exposes current redo log file. This is very efficient query and will not affect overall performance
     *
     * @return full path or NULL if an exception occurs.
     */
    String[] getCurrentRedoLogFileName();

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
     * @return average duration of Log Miner view query
     */
    Long getMaxDurationOfFetchingQuery();

    /**
     * Log Miner view query returns number of captured DML , Commit and Rollback. This is what we call a batch.
     * @return duration of the last batch fetching
     */
    Long getLastDurationOfFetchingQuery();

    /**
     * Log Miner view query returns number of captured DML , Commit and Rollback. This is what we call a batch.
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
    long getLastBatchProcessingDuration();

    /**
     * @return number of captured DL during last mining session
     */
    int getLastCapturedDmlCount();

    /**
     * Maximum number of entries in Log Miner view to fetch. This is used to set the diapason of the SCN in mining query.
     * If difference between "start SCN" and "end SCN" to mine exceeds this limit, end SCN will be set to "start SCN" + batchSize
     * @return the limit
     */
    int getBatchSize();

    /**
     * this gives ability to manipulate number of entries in Log Miner view to fetch.
     * It has limits to prevent abnormal values
     * @param size limit
     */
    void setBatchSize(int size);

    /**
     * @return number of milliseconds for connector to sleep before fetching another batch from the Log Miner view
     */
    Integer getMillisecondToSleepBetweenMiningQuery();

    /**
     * sets number of milliseconds for connector to sleep before fetching another batch from the Log Miner view
     * @param milliseconds to sleep
     */
    void setMillisecondToSleepBetweenMiningQuery(Integer milliseconds);

    /**
     * change sleeping time
     * @param increment true to add, false to deduct
     */
    void changeSleepingTime(boolean increment);

    void changeBatchSize(boolean increment);

    /**
     * this flag turns on recording of captured incremental changes. It creates an overhead on CPU and takes disk space
     * @param doRecording true - record
     */
    void setRecordMiningHistory(boolean doRecording);

    // record log mining history flag
    boolean getRecordMiningHistory();

    // *** following metrics work if RecordMiningHistory is true.

    // number of records in temp mining history table. It fluctuates from 0 to 10_000
    int getTempHistoryTableRecordsCounter();

    // number of records in current mining history table.
    int getCurrentHistoryTableRecordsCounter();

    // total number of records in the all mining history tables.
    long getTotalHistoryTableRecordsCounter();

    // remaining capacity of the queue which contains mining history records to persist
    int getRecordHistoryQueueCapacity();

    // get queue limit
    int getMiningHistoryQueueLimit();

    // hours to keep transaction in the buffer before abandoning
    int getHoursToKeepTransactionInBuffer();

    // set hours to keep transaction in the buffer before abandoning
    void setHoursToKeepTransactionInBuffer(int hours);

    // ***************** end of RecordMiningHistory metrics

    // returns max number of processed entries, captured from Log Miner view (DMLs, commits, rollbacks etc.) per second
    long getMaxBatchProcessingThroughput();

    // returns number of the last processed entries, captured from Log Miner view (DMLs, commits, rollbacks etc.) per second
    long getLastBatchProcessingThroughput();

    // returns average number of processed entries, captured from Log Miner view (DMLs, commits, rollbacks etc.) per second
    long getAverageBatchProcessingThroughput();

    // returns counter of registered network problems
    long getNetworkConnectionProblemsCounter();

    // reset metrics
    void reset();
}
