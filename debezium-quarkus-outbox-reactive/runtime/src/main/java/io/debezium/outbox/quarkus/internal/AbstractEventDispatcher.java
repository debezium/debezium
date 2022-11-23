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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.smallrye.mutiny.Uni;

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
    Mutiny.SessionFactory factory;

    /**
     * Debezium runtime configuration
     */
    @Inject
    DebeziumOutboxRuntimeConfig config;

    protected Uni<Void> persist(Map<String, Object> dataMap) {
        return factory.withSession(
                session -> session.withTransaction(
                        tx -> session.persist(dataMap)))
                .invoke(() -> LOGGER.debug("outbox event persisted"))
                .call(() -> this.removeFromOutbox(dataMap));

    }

    protected Uni<Integer> removeFromOutbox(Map<String, Object> dataMap) {
        if (config.removeAfterInsert) {
            LOGGER.debug("removing outbox event");
            return factory.withSession(
                    session -> session.withTransaction(
                            tx -> session.createQuery("delete from " + OUTBOX_ENTITY_FULLNAME + " where " + AGGREGATE_ID + "=:aggregateId")
                                    .setParameter("aggregateId", dataMap.get(AGGREGATE_ID)).executeUpdate()));
        }
        return Uni.createFrom().item(-1);
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
