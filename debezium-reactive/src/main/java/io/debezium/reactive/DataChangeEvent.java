/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

/**
 * A class that encapsulates a data change event coming from database and associates the {@link #complete()} operation with it.
 * It is expected that the consumer will receive the event from the pipeline and when the event is
 * durably processed it will signal this fact back to engine using {@link #complete()} call.
 *
 * @author Jiri Pechanec
 *
 */
public interface DataChangeEvent<T> {

    /**
     * @return A key for a change data capture event as {@link SourceRecord} received from Debezium connector.
     * The key is converted to a requested type.
     */
    public T getKey();

    /**
     * @return A value of a change data capture event as {@link SourceRecord} received from Debezium connector.
     * The value is converted to a requested type.
     */
    public T getValue();

    /**
     * Must be called when consumer durably processes the event in the pipeline. The engine will throw
     * an exception whenever the consumer tries to read a new event when an uncommitted event exists
     * or when the pipeline processing is finished without calling it.
     */
    public void complete();
}
