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
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.ConnectorStoppedEvent;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.runtime.events.Engine;
import io.debezium.runtime.events.PollingStartedEvent;
import io.debezium.runtime.events.PollingStoppedEvent;
import io.debezium.runtime.events.TasksStartedEvent;
import io.debezium.runtime.events.TasksStoppedEvent;
import io.quarkus.arc.Unremovable;

@ApplicationScoped
@Unremovable
public class DefaultStateHandler implements StateHandler {

    private final ConcurrentHashMap<EngineManifest, DebeziumStatus> statues = new ConcurrentHashMap<>();

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
    public ConnectorCallback connectorCallback(EngineManifest engineManifest, Debezium engine) {
        return new ConnectorCallback() {
            @Override
            public void connectorStarted() {
                changeState(engineManifest, DebeziumStatus.State.CREATING);
                connectorStarted
                        .select(Engine.Literal.of(engineManifest.id()))
                        .fire(new ConnectorStartedEvent(engine));
            }

            @Override
            public void connectorStopped() {
                changeState(engineManifest, DebeziumStatus.State.STOPPED);
                connectorStopped
                        .select(Engine.Literal.of(engineManifest.id()))
                        .fire(new ConnectorStoppedEvent(engine));
            }

            @Override
            public void taskStarted() {
                taskStarted
                        .select(Engine.Literal.of(engineManifest.id()))
                        .fire(new TasksStartedEvent(engine));
            }

            @Override
            public void taskStopped() {
                taskStopped
                        .select(Engine.Literal.of(engineManifest.id()))
                        .fire(new TasksStoppedEvent(engine));
            }

            @Override
            public void pollingStarted() {
                changeState(engineManifest, DebeziumStatus.State.POLLING);
                pollingStarted
                        .select(Engine.Literal.of(engineManifest.id()))
                        .fire(new PollingStartedEvent(engine));
            }

            @Override
            public void pollingStopped() {
                pollingStopped
                        .select(Engine.Literal.of(engineManifest.id()))
                        .fire(new PollingStoppedEvent(engine));
            }

            private void changeState(EngineManifest manifest, DebeziumStatus.State newState) {
                statues.put(manifest, new DebeziumStatus(newState));
            }
        };
    }

    @Override
    public CompletionCallback completionCallback(EngineManifest engineManifest, Debezium engine) {
        return (success, message, error) -> {
            statues.put(engineManifest, new DebeziumStatus(DebeziumStatus.State.STOPPED));
            completed
                    .select(Engine.Literal.of(engineManifest.id()))
                    .fire(new DebeziumCompletionEvent(success, message, error));
        };
    }

    @Override
    public DebeziumStatus get(EngineManifest engineManifest) {
        return this.statues.getOrDefault(engineManifest, new DebeziumStatus(DebeziumStatus.State.STOPPED));
    }

}
