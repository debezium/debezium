/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

import io.debezium.runtime.Debezium;

/**
 * This event is fired when the connector task is stopped.
 *
 * @author Chris Cranford
 */
public class TasksStoppedEvent extends AbstractDebeziumLifecycleEvent {
    public TasksStoppedEvent(Debezium engine) {
        super(engine);
    }
}
