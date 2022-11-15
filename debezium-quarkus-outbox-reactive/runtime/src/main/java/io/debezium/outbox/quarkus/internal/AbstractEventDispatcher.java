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

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.tuple.DynamicMapInstantiator;
import org.jboss.logging.Logger;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.smallrye.mutiny.Uni;

/**
 * Abstract base class for the Debezium Outbox {@link EventDispatcher} contract.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventDispatcher implements EventDispatcher {

    private static final Logger LOGGER = Logger.getLogger(AbstractEventDispatcher.class);

    protected static final String TIMESTAMP = "timestamp";
    protected static final String PAYLOAD = "payload";
    protected static final String TYPE = "type";
    protected static final String AGGREGATE_ID = "aggregateId";
    protected static final String AGGREGATE_TYPE = "aggregateType";

    @Inject
    Mutiny.SessionFactory factory;

    /**
     * Debezium runtime configuration
     */
    @Inject
    DebeziumOutboxRuntimeConfig config;

    protected Uni<Void> persist(Map<String, Object> dataMap) {
        LOGGER.infof("entered persist method on thread: " + Thread.currentThread().getName());
        return factory.withSession(
                session -> session.withTransaction(
                        tx -> session.persist(dataMap)))
                .invoke(() -> LOGGER.info("outbox event persisted"));
        // if (config.removeAfterInsert) {
        // session.delete(OUTBOX_ENTITY_FULLNAME, dataMap);
        // }
    }

    protected Map<String, Object> getDataMapFromEvent(ExportedEvent<?, ?> event) {
        final HashMap<String, Object> dataMap = new HashMap<>();
        dataMap.put(AGGREGATE_TYPE, event.getAggregateType());
        dataMap.put(AGGREGATE_ID, event.getAggregateId());
        dataMap.put(TYPE, event.getType());
        dataMap.put(PAYLOAD, event.getPayload());
        dataMap.put(TIMESTAMP, event.getTimestamp());
        dataMap.put(DynamicMapInstantiator.KEY, OUTBOX_ENTITY_FULLNAME);

        for (Map.Entry<String, Object> additionalFields : event.getAdditionalFieldValues().entrySet()) {
            if (dataMap.containsKey(additionalFields.getKey())) {
                LOGGER.error(
                        additionalFields.getKey());
                continue;
            }
            dataMap.put(additionalFields.getKey(), additionalFields.getValue());
        }

        return dataMap;
    }
}
