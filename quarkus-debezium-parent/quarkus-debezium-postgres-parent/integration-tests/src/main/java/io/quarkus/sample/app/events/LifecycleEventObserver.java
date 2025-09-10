/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.events;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import io.debezium.runtime.events.AbstractDebeziumLifecycleEvent;
import io.debezium.runtime.events.CaptureGroup;
import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.ConnectorStoppedEvent;
import io.debezium.runtime.events.PollingStartedEvent;
import io.debezium.runtime.events.PollingStoppedEvent;
import io.debezium.runtime.events.TasksStartedEvent;
import io.debezium.runtime.events.TasksStoppedEvent;

/**
 * A simple observer for Debezium lifecycle events, where they get recorded.
 *
 * @author Chris Cranford
 */
@ApplicationScoped
public class LifecycleEventObserver {

    private final List<AbstractDebeziumLifecycleEvent> defaultLifecycleEvents = new CopyOnWriteArrayList<>();
    private final List<AbstractDebeziumLifecycleEvent> alternativeLifecycleEvents = new CopyOnWriteArrayList<>();

    void defaultOnConnectorStarted(@Observes @CaptureGroup("default") ConnectorStartedEvent connectorStartedEvent) {
        defaultLifecycleEvents.add(connectorStartedEvent);
    }

    void defaultOnConnectorStopped(@Observes @CaptureGroup("default") ConnectorStoppedEvent connectorStoppedEvent) {
        defaultLifecycleEvents.add(connectorStoppedEvent);
    }

    void defaultOnTaskStarted(@Observes @CaptureGroup("default") TasksStartedEvent tasksStartedEvent) {
        defaultLifecycleEvents.add(tasksStartedEvent);
    }

    void defaultOnTaskStopped(@Observes @CaptureGroup("default") TasksStoppedEvent tasksStoppedEvent) {
        defaultLifecycleEvents.add(tasksStoppedEvent);
    }

    void defaultOnPollingStarted(@Observes @CaptureGroup("default") PollingStartedEvent pollingStartedEvent) {
        defaultLifecycleEvents.add(pollingStartedEvent);
    }

    void defaultOnPollingStopped(@Observes @CaptureGroup("default") PollingStoppedEvent pollingStoppedEvent) {
        defaultLifecycleEvents.add(pollingStoppedEvent);
    }

    void alternativeOnConnectorStarted(@Observes @CaptureGroup("alternative") ConnectorStartedEvent connectorStartedEvent) {
        alternativeLifecycleEvents.add(connectorStartedEvent);
    }

    void alternativeOnConnectorStopped(@Observes @CaptureGroup("alternative") ConnectorStoppedEvent connectorStoppedEvent) {
        alternativeLifecycleEvents.add(connectorStoppedEvent);
    }

    void alternativeOnTaskStarted(@Observes @CaptureGroup("alternative") TasksStartedEvent tasksStartedEvent) {
        alternativeLifecycleEvents.add(tasksStartedEvent);
    }

    void alternativeOnTaskStopped(@Observes @CaptureGroup("alternative") TasksStoppedEvent tasksStoppedEvent) {
        alternativeLifecycleEvents.add(tasksStoppedEvent);
    }

    void alternativeOnPollingStarted(@Observes @CaptureGroup("alternative") PollingStartedEvent pollingStartedEvent) {
        alternativeLifecycleEvents.add(pollingStartedEvent);
    }

    void alternativeOnPollingStopped(@Observes @CaptureGroup("alternative") PollingStoppedEvent pollingStoppedEvent) {
        alternativeLifecycleEvents.add(pollingStoppedEvent);
    }

    public List<AbstractDebeziumLifecycleEvent> getLifecycleEvents(String engine) {
        if (engine.equals("default")) {
            return defaultLifecycleEvents;
        }
        return alternativeLifecycleEvents;
    }

}
