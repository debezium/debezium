/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels.jmx;

import static io.debezium.pipeline.JmxUtils.registerMXBean;
import static io.debezium.pipeline.JmxUtils.unregisterMXBean;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanNotificationInfo;
import javax.management.NotificationBroadcasterSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.NotificationChannel;

public class JmxNotificationChannel extends NotificationBroadcasterSupport implements NotificationChannel, JmxNotificationChannelMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxNotificationChannel.class);
    private static final String CHANNEL_NAME = "jmx";
    private static final String DEBEZIUM_NOTIFICATION_TYPE = "debezium.notification";

    private static final List<Notification> NOTIFICATIONS = new ArrayList<>();

    private final AtomicLong notificationSequence = new AtomicLong(0);

    private CommonConnectorConfig connectorConfig;

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void init(CommonConnectorConfig connectorConfig) {

        this.connectorConfig = connectorConfig;

        registerMXBean(this, connectorConfig, "management", "notifications");

        LOGGER.info("Registration for Notification MXBean with the platform server is successfully");

    }

    @Override
    public void send(Notification notification) {

        NOTIFICATIONS.add(notification);

        sendNotification(buildJmxNotification(notification));
    }

    private javax.management.Notification buildJmxNotification(Notification notification) {

        javax.management.Notification n = new javax.management.Notification(
                DEBEZIUM_NOTIFICATION_TYPE,
                this,
                notificationSequence.getAndIncrement(),
                System.currentTimeMillis(),
                composeMessage(notification));

        n.setUserData(notification.toString());

        return n;
    }

    private String composeMessage(Notification notification) {
        return String.format("%s generated a notification", notification.getAggregateType());
    }

    @Override
    public void close() {

        unregisterMXBean(connectorConfig, "management", "notifications");
    }

    @Override
    public List<Notification> getNotifications() {
        return NOTIFICATIONS;
    }

    @Override
    public void reset() {

        NOTIFICATIONS.clear();
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {

        String[] types = new String[]{
                DEBEZIUM_NOTIFICATION_TYPE
        };

        String name = Notification.class.getName();
        String description = "Notification emitted by Debezium about its status";
        MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description);

        return new MBeanNotificationInfo[]{ info };
    }
}
