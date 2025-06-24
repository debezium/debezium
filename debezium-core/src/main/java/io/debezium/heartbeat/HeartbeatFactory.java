/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * A factory for creating the appropriate {@link Heartbeat} implementation based on the connector
 * type and its configured properties.
 *
 * @author Chris Cranford
 */
public class HeartbeatFactory<T extends DataCollectionId> {

    private final CommonConnectorConfig connectorConfig;
    private final TopicNamingStrategy<T> topicNamingStrategy;
    private final SchemaNameAdjuster schemaNameAdjuster;
    private final HeartbeatConnectionProvider connectionProvider;
    private final HeartbeatErrorHandler errorHandler;

    /**
     *  replaced by {@link #HeartbeatFactory()}
     */
    @Deprecated
    public HeartbeatFactory(CommonConnectorConfig connectorConfig, TopicNamingStrategy<T> topicNamingStrategy, SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicNamingStrategy, schemaNameAdjuster, null, null);
    }

    /**
     *  replaced by {@link #HeartbeatFactory()}
     */
    @Deprecated
    public HeartbeatFactory(CommonConnectorConfig connectorConfig, TopicNamingStrategy<T> topicNamingStrategy, SchemaNameAdjuster schemaNameAdjuster,
                            HeartbeatConnectionProvider connectionProvider, HeartbeatErrorHandler errorHandler) {
        this.connectorConfig = connectorConfig;
        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaNameAdjuster = schemaNameAdjuster;
        this.connectionProvider = connectionProvider;
        this.errorHandler = errorHandler;
    }

    public HeartbeatFactory() {
        this.connectorConfig = null;
        this.topicNamingStrategy = null;
        this.schemaNameAdjuster = null;
        this.connectionProvider = null;
        this.errorHandler = null;
    }

    /**
     *
     * @deprecated replaced by the {@link #create(CommonConnectorConfig, SchemaNameAdjuster, HeartbeatConnectionProvider, HeartbeatErrorHandler, String)}
     */
    @Deprecated
    public Heartbeat createHeartbeat() {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return Heartbeat.DEFAULT_NOOP_HEARTBEAT;
        }

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relConfig) {
            if (relConfig.getHeartbeatActionQuery() != null) {
                return new DatabaseHeartbeatImpl(
                        connectorConfig.getHeartbeatInterval(),
                        topicNamingStrategy.heartbeatTopic(),
                        connectorConfig.getLogicalName(),
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler,
                        schemaNameAdjuster);
            }
        }

        return new HeartbeatImpl(
                connectorConfig.getHeartbeatInterval(),
                topicNamingStrategy.heartbeatTopic(),
                connectorConfig.getLogicalName(),
                schemaNameAdjuster);
    }

    public Heartbeat create(CommonConnectorConfig connectorConfig,
                            SchemaNameAdjuster schemaNameAdjuster,
                            HeartbeatConnectionProvider connectionProvider,
                            HeartbeatErrorHandler errorHandler, String topicName) {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return Heartbeat.DEFAULT_NOOP_HEARTBEAT;
        }

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relConfig) {
            if (relConfig.getHeartbeatActionQuery() != null) {
                return new DatabaseHeartbeatImpl(
                        connectorConfig.getHeartbeatInterval(),
                        topicName,
                        connectorConfig.getLogicalName(),
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler,
                        schemaNameAdjuster);
            }
        }

        return new HeartbeatImpl(
                connectorConfig.getHeartbeatInterval(),
                topicName,
                connectorConfig.getLogicalName(),
                schemaNameAdjuster);
    }
}
