/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.internal;

import jakarta.enterprise.context.ApplicationScoped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.debezium.outbox.reactive.quarkus.DebeziumCustomCodec;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;

/**
 * The default application-scoped {@link EventDispatcher} implementation that is responsible
 * for observing {@link ExportedEvent} events and when detected, persists them to the
 * underlying database, allowing Debezium to capture and emit these as change events.
 *
 * @author Chris Cranford
 */
@ApplicationScoped
public class DefaultEventDispatcher extends AbstractEventDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultEventDispatcher.class);

    @Override
    @ConsumeEvent(value = "debezium-outbox", codec = DebeziumCustomCodec.class)
    public Uni<Void> onExportedEvent(Object event) {
        LOGGER.debug("An exported event was found for type {}", event.getClass().getName());
        return persist(getDataMapFromEvent((ExportedEvent<?, ?>) event));
    }
}
