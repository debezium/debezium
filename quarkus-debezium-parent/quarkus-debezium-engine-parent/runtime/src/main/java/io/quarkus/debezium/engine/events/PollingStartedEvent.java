/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine.events;

import io.debezium.runtime.Debezium;

/**
 * This event is fired when the Debezium engine begins polling for connector changes.
 *
 * @author Chris Cranford
 */
public class PollingStartedEvent extends AbstractDebeziumLifecycleEvent {
    public PollingStartedEvent(Debezium engine) {
        super(engine);
    }
}
