/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.api;

import java.util.Optional;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

/**
 * Holder interface for obtaining the configured sink consumer and its capabilities.
 * <p>
 * This Holder provides access to the sink-specific {@link DebeziumServerConsumer} instance
 * selected based on the {@code debezium.sink.type} configuration property. The actual consumer
 * is discovered via CDI by matching the configured sink type against {@code @Named} beans.
 * <p>
 * The Holder also exposes whether the selected consumer supports tombstone events, allowing
 * other components to adapt behavior accordingly.
 */
public interface ChangeConsumerHolder {
    DebeziumServerConsumer<CapturingEvents<BatchEvent>> get();

    Optional<Boolean> tombstoneSupport();
}
