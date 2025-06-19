/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine.events;

import io.debezium.runtime.Debezium;

/**
 * Abstract base class for all Debezium lifecycle events.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDebeziumLifecycleEvent {

    private final Debezium engine;

    public AbstractDebeziumLifecycleEvent(Debezium engine) {
        this.engine = engine;
    }

    public Debezium getEngine() {
        return engine;
    }
}
