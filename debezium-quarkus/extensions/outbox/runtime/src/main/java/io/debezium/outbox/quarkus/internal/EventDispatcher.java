/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

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

    public EventDispatcher() {
    }

    /**
     * An event handler for {@link ExportedEvent} events and will be called when the event fires.
     *
     * @param event the exported event
     */
    public void onExportedEvent(@Observes ExportedEvent<?, ?> event) {
        LOGGER.debug("An exported event was found for type {}", event.getType());

        // Create an OutboxEvent object based on the ExportedEvent interface
        final OutboxEvent outboxEvent = new OutboxEvent(
                event.getAggregateType(),
                (String) event.getAggregateId(),
                event.getType(),
                (JsonNode) event.getPayload(),
                event.getTimestamp());

        // We want the events table to remain empty; however this triggers both an INSERT and DELETE
        // in the database transaction log which is sufficient for Debezium to process the event.
        entityManager.persist(outboxEvent);
        entityManager.remove(outboxEvent);
    }
}
