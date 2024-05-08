/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import jakarta.enterprise.event.Observes;

import io.debezium.outbox.quarkus.ExportedEvent;

/**
 * Contract for a Debezium Outbox event dispatcher.
 *
 * @author Chris Cranford
 */
public interface EventDispatcher {
    /**
     * An event handler for {@link ExportedEvent} events and will be called when
     * the event fires.
     *
     * @param event
     *            the exported event
     */
    void onExportedEvent(@Observes ExportedEvent<?, ?> event);
}
