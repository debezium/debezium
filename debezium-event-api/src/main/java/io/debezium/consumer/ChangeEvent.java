/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.consumer;

import org.apache.kafka.connect.data.Struct;

/**
 * An event representing a change in a source. The application processing each event must call {@link #commit()} when it
 * has fully processed that event.
 * 
 * @author Randall Hauch
 */
public interface ChangeEvent {

    /**
     * Get the name of the topic in which this event was found.
     * 
     * @return the topic name; never null
     */
    String topic();

    /**
     * Get the key that describes the affected row, document, or data element. Note that the schema information for the
     * {@link Struct} (or any nested Struct) is available via that its {@link Struct#schema()} method.
     * 
     * @return the change event's key; may be null
     */
    Struct key();

    /**
     * Get the description of the change. Note that the schema information for the
     * {@link Struct} (or any nested Struct) is available via that its {@link Struct#schema()} method.
     * 
     * @return the change event's value; may be null if the row, document, or data element was deleted
     */
    Struct value();

    /**
     * Signal that this event has been fully processed.
     */
    void commit();
}
