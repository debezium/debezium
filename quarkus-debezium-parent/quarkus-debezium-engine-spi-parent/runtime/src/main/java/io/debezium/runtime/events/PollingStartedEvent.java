/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

import io.debezium.runtime.Debezium;

/**
 * This event is fired when the Debezium engine begins polling for connector changes.
 *
 * @author Chris Cranford
 */
public final class PollingStartedEvent extends AbstractDebeziumLifecycleEvent {
    public PollingStartedEvent(Debezium engine) {
        super(engine);
    }
}
