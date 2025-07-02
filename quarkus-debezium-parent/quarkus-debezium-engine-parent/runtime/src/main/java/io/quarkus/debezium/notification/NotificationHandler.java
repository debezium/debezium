/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

/**
 *
 * {@link NotificationHandler} is used to manage Debezium Notification
 *
 */
public interface NotificationHandler {

    /**
     * Define the handler availability {@param aggregateType}
     * @param aggregateType
     * @return
     */
    boolean isAvailable(String aggregateType);

    /**
     * handle the Debezium Notification
     * @param notification
     */
    void handle(io.debezium.pipeline.notification.Notification notification);
}
