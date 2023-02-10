/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.Map;

import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.hibernate.Session;

/**
 * Abstract base class for the Debezium Outbox {@link EventDispatcher} contract.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventDispatcher extends AbstractEventWriter<Void> implements EventDispatcher {

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
    protected Void persist(Map<String, Object> dataMap) {
        // Unwrap to Hibernate session and save
        Session session = entityManager.unwrap(Session.class);
        session.save(OUTBOX_ENTITY_FULLNAME, dataMap);
        session.setReadOnly(dataMap, true);
        remove(dataMap, session);

        return null;
    }

    private void remove(Map<String, Object> dataMap, Session session) {
        // Remove entity if the configuration deems doing so, leaving useful for debugging
        if (config.removeAfterInsert) {
            session.delete(OUTBOX_ENTITY_FULLNAME, dataMap);
        }
    }
}
