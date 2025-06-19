/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.arc.Unremovable;
import io.quarkus.debezium.engine.events.ConnectorStartedEvent;
import io.quarkus.debezium.engine.events.ConnectorStoppedEvent;
import io.quarkus.debezium.engine.events.DebeziumCompletionEvent;
import io.quarkus.debezium.engine.events.PollingStartedEvent;
import io.quarkus.debezium.engine.events.PollingStoppedEvent;
import io.quarkus.debezium.engine.events.TasksStartedEvent;
import io.quarkus.debezium.engine.events.TasksStoppedEvent;

@ApplicationScoped
@Unremovable
public class DefaultStateHandler implements StateHandler {

    private final AtomicReference<DebeziumStatus> status;

    @Inject
    Event<ConnectorStartedEvent> connectorStarted;
    @Inject
    Event<ConnectorStoppedEvent> connectorStopped;
    @Inject
    Event<TasksStartedEvent> taskStarted;
    @Inject
    Event<TasksStoppedEvent> taskStopped;
    @Inject
    Event<PollingStartedEvent> pollingStarted;
    @Inject
    Event<PollingStoppedEvent> pollingStopped;
    @Inject
    Event<DebeziumCompletionEvent> completed;

    private Debezium engine;

    public DefaultStateHandler() {
        this.status = new AtomicReference<>(new DebeziumStatus(DebeziumStatus.State.STOPPED));
    }

    @Override
    public ConnectorCallback connectorCallback() {
        return new ConnectorCallback() {
            @Override
            public void connectorStarted() {
                changeState(DebeziumStatus.State.CREATING);
                connectorStarted.fire(new ConnectorStartedEvent(engine));
            }

            @Override
            public void connectorStopped() {
                changeState(DebeziumStatus.State.STOPPED);
                connectorStopped.fire(new ConnectorStoppedEvent(engine));
            }

            @Override
            public void taskStarted() {
                taskStarted.fire(new TasksStartedEvent(engine));
            }

            @Override
            public void taskStopped() {
                taskStopped.fire(new TasksStoppedEvent(engine));
            }

            @Override
            public void pollingStarted() {
                changeState(DebeziumStatus.State.POLLING);
                pollingStarted.fire(new PollingStartedEvent(engine));
            }

            @Override
            public void pollingStopped() {
                pollingStopped.fire(new PollingStoppedEvent(engine));
            }

            private void changeState(DebeziumStatus.State newState) {
                status.set(new DebeziumStatus(newState));
            }
        };
    }

    @Override
    public CompletionCallback completionCallback() {
        return (success, message, error) -> {
            status.set(new DebeziumStatus(DebeziumStatus.State.STOPPED));
            completed.fire(new DebeziumCompletionEvent(success, message, error));
        };
    }

    @Override
    public DebeziumStatus get() {
        return this.status.get();
    }

    @Override
    public void setDebeziumEngine(Debezium engine) {
        this.engine = engine;
    }
}
