/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

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
     * @param record the Sink record descriptor
     */
    void enqueue(CollectionId collectionId, DebeziumSinkRecord record);

    DebeziumSinkRecord remove(CollectionId collectionId, DebeziumSinkRecord record);

    int size();

    void truncate(CollectionId collectionId, DebeziumSinkRecord record);

    /**
     * Polls for a batch of {@link DebeziumSinkRecord} that can be stored to the datastore.
     *
     * @return a batch of {@link DebeziumSinkRecord}
     */
    Batch poll();

}
