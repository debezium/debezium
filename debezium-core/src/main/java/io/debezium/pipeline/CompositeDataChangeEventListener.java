/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * Composite design pattern to aggregate multiple instances of {@link DataChangeEventListener}.
 *
 * @author Jiri Pechanec
 *
 */
public class CompositeDataChangeEventListener implements DataChangeEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeDataChangeEventListener.class);

    final List<DataChangeEventListener> elements = new ArrayList<>();

    public CompositeDataChangeEventListener(DataChangeEventListener... components) {
        Collections.addAll(this.elements, components);
    }

    @Override
    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) throws InterruptedException {
        for (DataChangeEventListener c : elements) {
            c.onEvent(source, offset, key, value);
        }
        LOGGER.debug("Created instance with {} elements", elements.size());
    }

    @Override
    public void onFilteredEvent(String event) {
        for (DataChangeEventListener c : elements) {
            c.onFilteredEvent(event);
        }
    }

    @Override
    public void onErroneousEvent(String event) {
        for (DataChangeEventListener c : elements) {
            c.onErroneousEvent(event);
        }
    }
}
