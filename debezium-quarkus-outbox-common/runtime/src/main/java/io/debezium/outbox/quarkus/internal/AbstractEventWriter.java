/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;

/**
 * Abstract event writer implementation, should be extended by concrete event dispatchers.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventWriter.class);

    protected static final String TIMESTAMP = "timestamp";
    protected static final String PAYLOAD = "payload";
    protected static final String TYPE = "type";
    protected static final String AGGREGATE_ID = "aggregateId";
    protected static final String AGGREGATE_TYPE = "aggregateType";

    protected abstract T persist(Map<String, Object> dataMap);

    protected Map<String, Object> getDataMapFromEvent(ExportedEvent<?, ?> event) {
        final Map<String, Object> dataMap = createDataMap(event);

        for (Map.Entry<String, Object> additionalFields : event.getAdditionalFieldValues().entrySet()) {
            if (dataMap.containsKey(additionalFields.getKey())) {
                LOGGER.error("Outbox entity already contains field with name '{}', additional field mapping skipped",
                        additionalFields.getKey());
                continue;
            }
            dataMap.put(additionalFields.getKey(), additionalFields.getValue());
        }

        return dataMap;
    }

    protected Map<String, Object> createDataMap(ExportedEvent<?, ?> event) {
        final Map<String, Object> dataMap = new HashMap<>();
        dataMap.put(AGGREGATE_TYPE, event.getAggregateType());
        dataMap.put(AGGREGATE_ID, event.getAggregateId());
        dataMap.put(TYPE, event.getType());
        dataMap.put(PAYLOAD, event.getPayload());
        dataMap.put(TIMESTAMP, event.getTimestamp());
        return dataMap;
    }
}
