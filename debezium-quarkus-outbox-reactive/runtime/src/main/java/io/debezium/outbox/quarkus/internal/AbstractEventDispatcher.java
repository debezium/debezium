/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;

/**
 * Abstract base class for the Debezium Outbox {@link EventDispatcher} contract.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventDispatcher implements EventDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventDispatcher.class);

    protected static final String TIMESTAMP = "timestamp";
    protected static final String PAYLOAD = "payload";
    protected static final String TYPE = "type";
    protected static final String AGGREGATE_ID = "aggregateId";
    protected static final String AGGREGATE_TYPE = "aggregateType";

    @Inject
    EntityManager entityManager;

    /**
     * Debezium runtime configuration
     */
    @Inject
    DebeziumOutboxRuntimeConfig config;

    /**
     * Persists the map of key/value pairs to the database.
     *
     * @param dataMap the data map, should never be {@code null}
     */
    protected void persist(Map<String, Object> dataMap) {
        // Unwrap to Hibernate session and save
        Session session = entityManager.unwrap(Session.class);
        session.save(OUTBOX_ENTITY_FULLNAME, dataMap);
        session.setReadOnly(dataMap, true);

        // Remove entity if the configuration deems doing so, leaving useful
        // for debugging
        if (config.removeAfterInsert) {
            session.delete(OUTBOX_ENTITY_FULLNAME, dataMap);
        }
    }

    protected Map<String, Object> getDataMapFromEvent(ExportedEvent<?, ?> event) {
        final HashMap<String, Object> dataMap = new HashMap<>();
        dataMap.put(AGGREGATE_TYPE, event.getAggregateType());
        dataMap.put(AGGREGATE_ID, event.getAggregateId());
        dataMap.put(TYPE, event.getType());
        dataMap.put(PAYLOAD, event.getPayload());
        dataMap.put(TIMESTAMP, event.getTimestamp());

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
}
