/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

import io.debezium.runtime.Debezium;

/**
 * This event is fired when Debezium engine stops a connector.
 *
 * @author Chris Cranford
 */
public class ConnectorStoppedEvent extends AbstractDebeziumLifecycleEvent {
    public ConnectorStoppedEvent(Debezium engine) {
        super(engine);
    }
}
