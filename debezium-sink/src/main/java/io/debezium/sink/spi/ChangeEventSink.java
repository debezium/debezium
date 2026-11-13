/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.spi;

import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.common.annotation.Incubating;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;

/**
 * A change event sink that consumes events from one or more Kafka topics.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ChangeEventSink extends AutoCloseable {

    /**
     * Executes this sink.
     *
     * @param records the sink records, never {@code null}
     */
    List<Batch> put(Collection<SinkRecord> records);

    /**
     * Returns the CollectionId instance for the given collection name.
     *
     * @param collectionName the collection name
     * @return the collection id
     */
    CollectionId getCollectionId(String collectionName);

    /**
     * Polls all remaining {@link io.debezium.sink.DebeziumSinkRecord}s from the buffer as {@link Batch}.
     *
     * @return a list of {@link Batch}es of {@link io.debezium.sink.DebeziumSinkRecord}
     */
    List<Batch> forcePoll();

    /**
     * This is the main entry point called by the sink connector task's
     * {@link org.apache.kafka.connect.sink.SinkTask#put(Collection)} method. It combines
     * {@link #put(Collection)} (conversion and buffering) with batch writing in a single call.
     * Records that fail conversion or writing are reported to the configured error reporter
     * rather than propagating exceptions, unless the error is classified as retriable.
     *
     * @param records the Kafka Connect sink records to process, never {@code null}
     */
    void execute(Collection<SinkRecord> records);

}
