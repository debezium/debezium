/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;

/**
 * This interface is used to provide custom write channels for the Debezium notification feature:
 *
 * Implementations must:
 * define the name of the channel in {@link #name()},
 * initialize specific configuration/variables/connections in the {@link #init(CommonConnectorConfig connectorConfig)} method,
 * implement send of the notification on the channel in the {@link #send(Notification notification)} method.
 * Close all allocated resources int the {@link #close()} method.
 *
 * @author Mario Fiore Vitale
 */
public interface NotificationChannel {

    void init(CommonConnectorConfig config);

    String name();

    void send(Notification notification);

    void close();
}
