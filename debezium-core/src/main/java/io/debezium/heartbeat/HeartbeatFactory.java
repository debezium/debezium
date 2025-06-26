/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import static io.debezium.config.CommonConnectorConfig.TOPIC_NAMING_STRATEGY;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Strings;

/**
 * Default factory for creating the appropriate {@link Heartbeat} implementation based on the connector
 * type and its configured properties in the Debezium context.
 *
 * @author Chris Cranford
 */
public class HeartbeatFactory<T extends DataCollectionId> implements DebeziumHeartbeatFactory {

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
     * @deprecated replaced by the {@link DebeziumHeartbeatFactory#getScheduledHeartbeat(CommonConnectorConfig, HeartbeatConnectionProvider, HeartbeatErrorHandler, ChangeEventQueue)}
     */
    @Deprecated
    public Heartbeat createHeartbeat() {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return Heartbeat.DEFAULT_NOOP_HEARTBEAT;
        }

        DefaultHeartbeat heartbeat = new DefaultHeartbeat(
                connectorConfig.getHeartbeatInterval(),
                topicNamingStrategy.heartbeatTopic(),
                connectorConfig.getLogicalName(),
                schemaNameAdjuster);

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relConfig) {
            if (!Strings.isNullOrBlank(relConfig.getHeartbeatActionQuery())) {

                return new CompositeHeartbeat(heartbeat, new DatabaseHeartbeat(
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler));
            }
        }

        return heartbeat;
    }

    @Override
    public ScheduledHeartbeat getScheduledHeartbeat(CommonConnectorConfig connectorConfig,
                                                    HeartbeatConnectionProvider connectionProvider,
                                                    HeartbeatErrorHandler errorHandler,
                                                    ChangeEventQueue<DataChangeEvent> queue) {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return () -> false;
        }

        ScheduledHeartbeat heartbeat = new DefaultHeartbeat(
                connectorConfig.getHeartbeatInterval(),
                connectorConfig.getTopicNamingStrategy(TOPIC_NAMING_STRATEGY).heartbeatTopic(),
                connectorConfig.getLogicalName(),
                connectorConfig.schemaNameAdjuster(), queue);

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relConfig) {
            if (!Strings.isNullOrBlank(relConfig.getHeartbeatActionQuery())) {

                return new CompositeHeartbeat(heartbeat, new DatabaseHeartbeat(
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler));
            }
        }

        return heartbeat;
    }

}
