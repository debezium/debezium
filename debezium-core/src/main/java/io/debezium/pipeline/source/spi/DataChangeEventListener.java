/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.EventDispatcher;

/**
 * A class invoked by {@link EventDispatcher} whenever an event is available for processing.
 *
 * @author Jiri Pechanec
 *
 */
public interface DataChangeEventListener {

    void onEvent(String event);
    void onSkippedEvent(String event);

    static DataChangeEventListener NO_OP = new DataChangeEventListener() {
        @Override
        public void onSkippedEvent(String event) {
        }

        @Override
        public void onEvent(String event) {
        }
    };
}
