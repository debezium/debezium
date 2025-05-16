/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.runtime.DebeziumStatus;

class DefaultStateHandler implements StateHandler {
    private final AtomicReference<DebeziumStatus> status;

    DefaultStateHandler() {
        this.status = new AtomicReference<>(new DebeziumStatus(DebeziumStatus.State.CREATING));
    }

    @Override
    public ConnectorCallback connectorCallback() {
        return new ConnectorCallback() {
            public void pollingStarted() {
                status.set(new DebeziumStatus(DebeziumStatus.State.POLLING));
            }
        };
    }

    @Override
    public CompletionCallback completionCallback() {
        return (success, message, error) -> status.set(new DebeziumStatus(DebeziumStatus.State.STOPPED));
    }

    @Override
    public DebeziumStatus get() {
        return this.status.get();
    }

}
