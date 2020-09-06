/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.HashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;

/**
 * An application-scope component that is responsible for observing {@link ExportedEvent} events and when
 * detected, persists those events to the underlying database allowing Debezium to then capture and emit
 * those change events.
 *
 * @author Chris Cranford
 */
@ApplicationScoped
public class EventDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatcher.class);

    @Inject
    EntityManager entityManager;

    /**
     * Debezium runtime configuration
     */
    @Inject
    DebeziumOutboxRuntimeConfig config;

    /**
     * An event handler for {@link ExportedEvent} events and will be called when the event fires.
     *
     * @param event the exported event
     */
    public void onExportedEvent(@Observes ExportedEvent<?, ?> event) {
        LOGGER.debug("An exported event was found for type {}", event.getType());

        // Define the entity map-mode object using property names and values
        final HashMap<String, Object> dataMap = new HashMap<>();
        dataMap.put("aggregateType", event.getAggregateType());
        dataMap.put("aggregateId", event.getAggregateId());
        dataMap.put("type", event.getType());
        dataMap.put("payload", event.getPayload());
        dataMap.put("timestamp", event.getTimestamp());

        // Unwrap to Hibernate session and save
        Session session = entityManager.unwrap(Session.class);
        session.save(OUTBOX_ENTITY_FULLNAME, dataMap);

        // Remove entity if the configuration deems doing so, leaving useful for debugging
        if (config.removeAfterInsert) {
            session.delete(OUTBOX_ENTITY_FULLNAME, dataMap);
        }
    }
}
