/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.standalone.quarkus;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

import io.debezium.engine.DebeziumEngine;
import io.debezium.standalone.quarkus.events.ConnectorCompletedEvent;
import io.debezium.standalone.quarkus.events.ConnectorStartedEvent;
import io.debezium.standalone.quarkus.events.ConnectorStoppedEvent;
import io.debezium.standalone.quarkus.events.TaskStartedEvent;
import io.debezium.standalone.quarkus.events.TaskStoppedEvent;

/**
 * The server lifecycle listener that published CDI events based on the lifecycle changes and also provides
 * Microprofile Health information.
 *
 * @author Jiri Pechanec
 *
 */
@Liveness
@ApplicationScoped
public class ConnectorLifecycle implements HealthCheck, DebeziumEngine.ConnectorCallback, DebeziumEngine.CompletionCallback {

    private volatile boolean live = false;

    @Inject
    Event<ConnectorStartedEvent> connectorStartedEvent;

    @Inject
    Event<ConnectorStoppedEvent> connectorStoppedEvent;

    @Inject
    Event<TaskStartedEvent> taskStartedEvent;

    @Inject
    Event<TaskStoppedEvent> taskStoppedEvent;

    @Inject
    Event<ConnectorCompletedEvent> connectorCompletedEvent;

    @Override
    public void connectorStarted() {
        connectorStartedEvent.fire(new ConnectorStartedEvent());
    }

    @Override
    public void connectorStopped() {
        connectorStoppedEvent.fire(new ConnectorStoppedEvent());
    }

    @Override
    public void taskStarted() {
        taskStartedEvent.fire(new TaskStartedEvent());
        live = true;
    }

    @Override
    public void taskStopped() {
        taskStoppedEvent.fire(new TaskStoppedEvent());
    }

    @Override
    public void handle(boolean success, String message, Throwable error) {
        connectorCompletedEvent.fire(new ConnectorCompletedEvent(success, message, error));
        live = false;
    }

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("debezium").state(live).build();
    }

}
