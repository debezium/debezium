/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.NotificationChannel;

@ApplicationScoped
public class QuarkusNotificationChannel implements NotificationChannel {

    private final Instance<NotificationHandler> notificationHandlers;

    public QuarkusNotificationChannel() {
        this.notificationHandlers = CDI.current().select(NotificationHandler.class);
    }

    public QuarkusNotificationChannel(Instance<NotificationHandler> notificationHandlers) {
        this.notificationHandlers = notificationHandlers;
    }

    @Override
    public void init(CommonConnectorConfig config) {
        // ignore
    }

    @Override
    public String name() {
        return "quarkus";
    }

    @Override
    public void send(Notification notification) {
        notificationHandlers
                .stream()
                .filter(handler -> handler.isAvailable(notification.getAggregateType()))
                .forEach(handler -> handler.handle(notification));
    }

    @Override
    public void close() {
        // ignore
    }
}
