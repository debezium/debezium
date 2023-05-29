/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels.jmx;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.debezium.pipeline.JmxUtils.registerMXBean;

public class JmxNotificationChannel implements NotificationChannel, JmxNotificationChannelMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxNotificationChannel.class);
    private static final String CHANNEL_NAME = "jmx";

    private static final List<Notification> NOTIFICATIONS = new ArrayList<>();

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void init(CommonConnectorConfig connectorConfig) {

        registerMXBean(this, connectorConfig, "notifications");

        LOGGER.info("Registration for Notification MXBean with the platform server is successfully");

    }

    @Override
    public void send(Notification notification) {

        NOTIFICATIONS.add(notification);
    }

    @Override
    public void close() {

    }

    @Override
    public List<Notification> getNotifications() {
        return NOTIFICATIONS;
    }

    @Override
    public void reset() {

        NOTIFICATIONS.clear();
    }
}
