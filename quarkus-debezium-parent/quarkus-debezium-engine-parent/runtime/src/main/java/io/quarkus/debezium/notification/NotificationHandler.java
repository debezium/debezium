/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

public interface NotificationHandler {
    boolean isAvailable(String aggregateType);

    void handle(io.debezium.pipeline.notification.Notification notification);
}
