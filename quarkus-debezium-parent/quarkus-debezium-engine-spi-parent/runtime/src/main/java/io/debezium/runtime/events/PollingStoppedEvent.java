/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

import io.debezium.runtime.Debezium;

/**
 * This event is fired when Debezium engine stops polling the connector for changes.
 *
 * @author Chris Cranford
 */
public final class PollingStoppedEvent extends AbstractDebeziumLifecycleEvent {
    public PollingStoppedEvent(Debezium engine) {
        super(engine);
    }
}
