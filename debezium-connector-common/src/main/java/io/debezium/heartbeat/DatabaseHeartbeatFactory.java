/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.heartbeat;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;

public class DatabaseHeartbeatFactory implements DebeziumHeartbeatFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHeartbeatFactory.class);

    @Override
    public Optional<Heartbeat> getHeartbeat(CommonConnectorConfig connectorConfig,
                                            HeartbeatConnectionProvider connectionProvider,
                                            HeartbeatErrorHandler errorHandler,
                                            ChangeEventQueue<DataChangeEvent> queue) {
        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relConfig) {
            if (!Strings.isNullOrBlank(relConfig.getHeartbeatActionQuery())) {
                if (connectionProvider == null) {
                    LOGGER.warn("The '{}' option is configured but the connector does not provide a heartbeat connection; " +
                            "the heartbeat action query will not be executed",
                            DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME);
                    return Optional.empty();
                }
                return Optional.of(new DatabaseHeartbeatImpl(
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler));
            }
        }

        return Optional.empty();
    }
}
