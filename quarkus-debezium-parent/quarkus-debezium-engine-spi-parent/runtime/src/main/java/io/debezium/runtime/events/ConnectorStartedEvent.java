/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

import io.debezium.runtime.Debezium;

/**
 * This event is fired when the Debezium engine starts a connector.
 *
 * @author Chris Cranford
 */
public class ConnectorStartedEvent extends AbstractDebeziumLifecycleEvent {
    public ConnectorStartedEvent(Debezium engine) {
        super(engine);
    }
}
