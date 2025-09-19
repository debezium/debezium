/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.events.CaptureGroup;
import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.ConnectorStoppedEvent;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.runtime.events.PollingStartedEvent;
import io.debezium.runtime.events.PollingStoppedEvent;
import io.debezium.runtime.events.TasksStartedEvent;
import io.debezium.runtime.events.TasksStoppedEvent;
import io.quarkus.arc.Unremovable;

@ApplicationScoped
@Unremovable
public class DefaultStateHandler implements StateHandler {

    private final ConcurrentHashMap<io.debezium.runtime.CaptureGroup, DebeziumStatus> statues = new ConcurrentHashMap<>();

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

    @Override
    public ConnectorCallback connectorCallback(io.debezium.runtime.CaptureGroup captureGroup, Debezium engine) {
        return new ConnectorCallback() {
            @Override
            public void connectorStarted() {
                changeState(captureGroup, DebeziumStatus.State.CREATING);
                connectorStarted
                        .select(CaptureGroup.Literal.of(captureGroup.id()))
                        .fire(new ConnectorStartedEvent(engine));
            }

            @Override
            public void connectorStopped() {
                changeState(captureGroup, DebeziumStatus.State.STOPPED);
                connectorStopped
                        .select(CaptureGroup.Literal.of(captureGroup.id()))
                        .fire(new ConnectorStoppedEvent(engine));
            }

            @Override
            public void taskStarted() {
                taskStarted
                        .select(CaptureGroup.Literal.of(captureGroup.id()))
                        .fire(new TasksStartedEvent(engine));
            }

            @Override
            public void taskStopped() {
                taskStopped
                        .select(CaptureGroup.Literal.of(captureGroup.id()))
                        .fire(new TasksStoppedEvent(engine));
            }

            @Override
            public void pollingStarted() {
                changeState(captureGroup, DebeziumStatus.State.POLLING);
                pollingStarted
                        .select(CaptureGroup.Literal.of(captureGroup.id()))
                        .fire(new PollingStartedEvent(engine));
            }

            @Override
            public void pollingStopped() {
                pollingStopped
                        .select(CaptureGroup.Literal.of(captureGroup.id()))
                        .fire(new PollingStoppedEvent(engine));
            }

            private void changeState(io.debezium.runtime.CaptureGroup group, DebeziumStatus.State newState) {
                statues.put(group, new DebeziumStatus(newState));
            }
        };
    }

    @Override
    public CompletionCallback completionCallback(io.debezium.runtime.CaptureGroup captureGroup, Debezium engine) {
        return (success, message, error) -> {
            statues.put(captureGroup, new DebeziumStatus(DebeziumStatus.State.STOPPED));
            completed
                    .select(CaptureGroup.Literal.of(captureGroup.id()))
                    .fire(new DebeziumCompletionEvent(success, message, error));
        };
    }

    @Override
    public DebeziumStatus get(io.debezium.runtime.CaptureGroup captureGroup) {
        return this.statues.getOrDefault(captureGroup, new DebeziumStatus(DebeziumStatus.State.STOPPED));
    }

}
