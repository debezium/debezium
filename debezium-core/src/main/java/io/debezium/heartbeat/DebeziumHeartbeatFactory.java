/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.pipeline.DataChangeEvent;

/**
 *
 * factory interface for creating the appropriate {@link Heartbeat} implementation based on the connector
 * and context
 */
public interface DebeziumHeartbeatFactory {
    ScheduledHeartbeat getScheduledHeartbeat(CommonConnectorConfig connectorConfig,
                                             HeartbeatConnectionProvider connectionProvider,
                                             HeartbeatErrorHandler errorHandler, ChangeEventQueue<DataChangeEvent> queue);
}
