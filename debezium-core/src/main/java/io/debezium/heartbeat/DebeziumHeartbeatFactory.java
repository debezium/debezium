/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

public interface DebeziumHeartbeatFactory {
    Heartbeat create(CommonConnectorConfig connectorConfig,
                     HeartbeatConnectionProvider connectionProvider,
                     HeartbeatErrorHandler errorHandler, ChangeEventQueue<DataChangeEvent> queue);
}
