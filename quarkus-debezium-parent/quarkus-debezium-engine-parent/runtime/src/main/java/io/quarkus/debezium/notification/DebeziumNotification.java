/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import java.util.Map;

public record DebeziumNotification(String id, String aggregateType, String type, Map<String, String> additionalData, Long timestamp) implements Notification {
    public static DebeziumNotification from(io.debezium.pipeline.notification.Notification notification) {
        return new DebeziumNotification(
                notification.getId(),
                notification.getAggregateType(),
                notification.getType(),
                notification.getAdditionalData(),
                notification.getTimestamp());
    }
}
