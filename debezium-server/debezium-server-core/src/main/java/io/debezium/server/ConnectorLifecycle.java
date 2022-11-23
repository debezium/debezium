/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.server.events.ConnectorStoppedEvent;
import io.debezium.server.events.TaskStartedEvent;
import io.debezium.server.events.TaskStoppedEvent;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorLifecycle.class);

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
        LOGGER.debug("Connector started");
        connectorStartedEvent.fire(new ConnectorStartedEvent());
    }

    @Override
    public void connectorStopped() {
        LOGGER.debug("Connector stopped");
        connectorStoppedEvent.fire(new ConnectorStoppedEvent());
    }

    @Override
    public void taskStarted() {
        LOGGER.debug("Task started");
        taskStartedEvent.fire(new TaskStartedEvent());
        live = true;
    }

    @Override
    public void taskStopped() {
        LOGGER.debug("Task stopped");
        taskStoppedEvent.fire(new TaskStoppedEvent());
    }

    @Override
    public void handle(boolean success, String message, Throwable error) {
        String logMessage = String.format("Connector completed: success = '%s', message = '%s', error = '%s'", success, message, error);
        if (success) {
            LOGGER.info(logMessage);
        }
        else {
            LOGGER.error(logMessage);
        }
        connectorCompletedEvent.fire(new ConnectorCompletedEvent(success, message, error));
        live = false;
    }

    @Override
    public HealthCheckResponse call() {
        LOGGER.trace("Healthcheck called - live = '{}'", live);
        return HealthCheckResponse.named("debezium").status(live).build();
    }

}
