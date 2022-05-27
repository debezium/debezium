/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Strings;

/**
 * A factory for creating the appropriate {@link Heartbeat} implementation based on the connector
 * type and its configured properties.
 *
 * @author Chris Cranford
 */
public class HeartbeatFactory<T extends DataCollectionId> {

    private final CommonConnectorConfig connectorConfig;
    private final TopicSelector<T> topicSelector;
    private final SchemaNameAdjuster schemaNameAdjuster;
    private final HeartbeatConnectionProvider connectionProvider;
    private final HeartbeatErrorHandler errorHandler;

    public HeartbeatFactory(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector, SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicSelector, schemaNameAdjuster, null, null);
    }

    public HeartbeatFactory(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector, SchemaNameAdjuster schemaNameAdjuster,
                            HeartbeatConnectionProvider connectionProvider, HeartbeatErrorHandler errorHandler) {
        this.connectorConfig = connectorConfig;
        this.topicSelector = topicSelector;
        this.schemaNameAdjuster = schemaNameAdjuster;

        this.connectionProvider = connectionProvider;
        this.errorHandler = errorHandler;
    }

    public Heartbeat createHeartbeat() {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return Heartbeat.DEFAULT_NOOP_HEARTBEAT;
        }

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig) {
            RelationalDatabaseConnectorConfig relConfig = (RelationalDatabaseConnectorConfig) connectorConfig;
            if (!Strings.isNullOrBlank(relConfig.getHeartbeatActionQuery())) {
                return new DatabaseHeartbeatImpl(
                        connectorConfig.getHeartbeatInterval(),
                        topicSelector.getHeartbeatTopic(),
                        connectorConfig.getLogicalName(),
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler,
                        schemaNameAdjuster);
            }
        }

        return new HeartbeatImpl(
                connectorConfig.getHeartbeatInterval(),
                topicSelector.getHeartbeatTopic(),
                connectorConfig.getLogicalName(),
                schemaNameAdjuster);
    }
}
