/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;

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
    // Uni<Void> onExportedEvent(@Observes ExportedEvent<?, ?> event);

    // Uni<Void> onExportedEvent(@ObservesAsync ExportedEvent<?, ?> event);

    @ConsumeEvent("debezium-outbox")
    Uni<Void> onExportedEvent(ExportedEvent<?, ?> event);
}
