/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * A class invoked by {@link EventDispatcher} whenever an event is available for processing.
 *
 * @author Jiri Pechanec
 *
 */
public interface DataChangeEventListener {

    /**
     * Invoked if an event is processed for a captured table.
     */
    void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value);

    /**
     * Invoked for events pertaining to non-whitelisted tables.
     */
    void onFilteredEvent(String event);

    static DataChangeEventListener NO_OP = new DataChangeEventListener() {
        @Override
        public void onFilteredEvent(String event) {
        }

        @Override
        public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        }
    };
}
