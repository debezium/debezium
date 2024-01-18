/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;

public class LogNotificationChannel implements NotificationChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogNotificationChannel.class);
    private static final String LOG_PREFIX = "[Notification Service] ";
    public static final String CHANNEL_NAME = "log";

    @Override
    public void init(CommonConnectorConfig config) {
    }

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void send(Notification notification) {

        LOGGER.info("{} {}", LOG_PREFIX, notification);
    }

    @Override
    public void close() {
    }
}
