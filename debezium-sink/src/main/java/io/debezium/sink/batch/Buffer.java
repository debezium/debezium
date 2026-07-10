/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.List;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * An interface for implementing several kind of batches
 *
 * @author rk3rn3r
 */
public interface Buffer {

    /**
     * Add a {@link DebeziumSinkRecord} to the internal buffer.
     * @param record the sink record
     */
    void enqueue(CollectionId collectionId, DebeziumSinkRecord record);

    /**
     * Returns the size of the buffer.
     *
     * @return the size of the buffer
     */
    int size();

    /**
     * Records a truncate event in the buffer.
     *
     * TODO: truncate specific code should eventually be moved into #enqueue()
     *
     * @param collectionId the collection id
     * @param record the truncate record
     */
    void truncate(CollectionId collectionId, DebeziumSinkRecord record);

    /**
     * Polls for {@link Batch}es of {@link DebeziumSinkRecord} that can be stored to the datastore.
     *
     * @return a list of {@link Batch}es of {@link DebeziumSinkRecord}
     */
    List<Batch> poll();

    /**
     * Polls all remaining {@link DebeziumSinkRecord}s in the buffer.
     *
     * @return a list of {@link Batch}es of {@link DebeziumSinkRecord}
     */
    List<Batch> forcePoll();

}
