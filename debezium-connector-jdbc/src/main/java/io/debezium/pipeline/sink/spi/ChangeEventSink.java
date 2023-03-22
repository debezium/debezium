/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.sink.spi;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.common.annotation.Incubating;

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
     * @param record the sink record, never {@code null}
     */
    void execute(SinkRecord record);
}
