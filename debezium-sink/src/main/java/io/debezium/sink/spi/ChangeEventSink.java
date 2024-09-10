/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.spi;

import java.util.Collection;
import java.util.Optional;

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
    Batch put(Collection<SinkRecord> records);

    Optional<CollectionId> getCollectionId(String collectionName);
}
