/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels.jmx;

import java.util.List;

import io.debezium.pipeline.notification.Notification;

public interface JmxNotificationChannelMXBean {

    List<Notification> getNotifications();

    void reset();
}
