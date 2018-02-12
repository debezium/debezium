/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A class that encapsulates {@link SourceRecord} and associates the {@link #commit()} operation with it.
 * It is expected that the consumer will receive the event from the pipeline and when the event is
 * durably processed it will signal this fact back to engine using {@link #commit()} call.
 *
 * @author Jiri Pechanec
 *
 */
public interface DebeziumEvent {

    /**
     * @return Change data capture event as {@link SourceRecord} received from Debezium connector.
     */
    public SourceRecord getRecord();

    /**
     * Must be called when consumer durably processes the event in the pipeline. The engine will throw
     * an exception whenever the consumer tries to read a new event when an uncommitted event exists
     * or when the pipeline processing is finished without calling it.
     */
    public void commit();
}
