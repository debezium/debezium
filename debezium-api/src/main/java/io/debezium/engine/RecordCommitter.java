package io.debezium.engine;

public interface RecordCommitter<R> {
    /**
     * Marks a single record as processed, must be called for each
     * record.
     *
     * @param record the record to commit
     */
    void markProcessed(R record) throws InterruptedException;

    /**
     * Marks a batch as finished, this may result in committing offsets/flushing
     * data.
     * <p>
     * Should be called when a batch of records is finished being processed.
     */
    void markBatchFinished();
}