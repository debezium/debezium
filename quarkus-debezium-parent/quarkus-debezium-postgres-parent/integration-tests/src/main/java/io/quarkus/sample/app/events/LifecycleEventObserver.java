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

    private final List<Object> lifecycleEvents = new CopyOnWriteArrayList<>();

    void onConnectorStarted(@Observes ConnectorStartedEvent connectorStartedEvent) {
        lifecycleEvents.add(connectorStartedEvent);
    }

    void onConnectorStopped(@Observes ConnectorStoppedEvent connectorStoppedEvent) {
        lifecycleEvents.add(connectorStoppedEvent);
    }

    void onTaskStarted(@Observes TasksStartedEvent tasksStartedEvent) {
        lifecycleEvents.add(tasksStartedEvent);
    }

    void onTaskStopped(@Observes TasksStoppedEvent tasksStoppedEvent) {
        lifecycleEvents.add(tasksStoppedEvent);
    }

    void onPollingStarted(@Observes PollingStartedEvent pollingStartedEvent) {
        lifecycleEvents.add(pollingStartedEvent);
    }

    void onPollingStopped(@Observes PollingStoppedEvent pollingStoppedEvent) {
        lifecycleEvents.add(pollingStoppedEvent);
    }

    public List<Object> getLifecycleEvents() {
        return lifecycleEvents;
    }

}
