/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.Map;

import jakarta.inject.Inject;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.tuple.DynamicMapInstantiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.debezium.outbox.quarkus.internal.AbstractEventWriter;
import io.smallrye.mutiny.Uni;

/**
 * Abstract base class for the Debezium Outbox {@link EventDispatcher} contract.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventDispatcher extends AbstractEventWriter<Uni<Void>> implements EventDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventDispatcher.class);

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

    @Override
    protected Map<String, Object> createDataMap(ExportedEvent<?, ?> event) {
        final Map<String, Object> dataMap = super.createDataMap(event);
        dataMap.put(DynamicMapInstantiator.KEY, OUTBOX_ENTITY_FULLNAME);
        return dataMap;
    }

}
