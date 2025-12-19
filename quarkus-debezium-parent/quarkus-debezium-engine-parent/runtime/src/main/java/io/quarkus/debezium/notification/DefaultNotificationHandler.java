/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.debezium.pipeline.notification.Notification;

@ApplicationScoped
public class DefaultNotificationHandler implements NotificationHandler {

    private final Event<DebeziumNotification> debeziumNotification;

    @Inject
    public DefaultNotificationHandler(Event<DebeziumNotification> debeziumNotification) {
        this.debeziumNotification = debeziumNotification;
    }

    @Override
    public boolean isAvailable(String aggregateType) {
        return true;
    }

    @Override
    public void handle(Notification notification) {
        debeziumNotification.fire(DebeziumNotification.from(notification));
    }
}
