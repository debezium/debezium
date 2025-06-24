/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.schema.SchemaNameAdjuster;

public interface HeartbeatsFactory {
    Heartbeat create(CommonConnectorConfig connectorConfig,
                     SchemaNameAdjuster schemaNameAdjuster,
                     HeartbeatConnectionProvider connectionProvider,
                     HeartbeatErrorHandler errorHandler, String topicName);
}
