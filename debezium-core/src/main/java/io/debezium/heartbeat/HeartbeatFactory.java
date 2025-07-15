/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import static io.debezium.config.CommonConnectorConfig.TOPIC_NAMING_STRATEGY;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

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
    private final ServiceLoader<DebeziumHeartbeatFactory> externalHeartbeatFactory = ServiceLoader.load(DebeziumHeartbeatFactory.class);
    private final List<DebeziumHeartbeatFactory> heartbeatFactories;

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
        this.heartbeatFactories = new ArrayList<>(externalHeartbeatFactory
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList());
        this.heartbeatFactories.add(new DatabaseHeartbeatFactory());
    }

    public HeartbeatFactory() {
        this.connectorConfig = null;
        this.topicNamingStrategy = null;
        this.schemaNameAdjuster = null;
        this.connectionProvider = null;
        this.errorHandler = null;
        this.heartbeatFactories = externalHeartbeatFactory
                .stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toList());
        this.heartbeatFactories.add(new DatabaseHeartbeatFactory());
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

        ScheduledHeartbeat scheduledHeartbeat = new HeartbeatImpl(
                connectorConfig.getHeartbeatInterval(),
                topicNamingStrategy.heartbeatTopic(),
                connectorConfig.getLogicalName(),
                schemaNameAdjuster);

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig relConfig) {
            if (!Strings.isNullOrBlank(relConfig.getHeartbeatActionQuery())) {

                return new CompositeHeartbeat(scheduledHeartbeat, new DatabaseHeartbeatImpl(
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler));
            }
        }

        return scheduledHeartbeat;
    }

    /**
     *
     * Create a heartbeat instance that can be scheduled with some delay based on {@link CommonConnectorConfig}
     *
     */
    @Override
    public ScheduledHeartbeat getScheduledHeartbeat(CommonConnectorConfig connectorConfig,
                                                    HeartbeatConnectionProvider connectionProvider,
                                                    HeartbeatErrorHandler errorHandler,
                                                    ChangeEventQueue<DataChangeEvent> queue) {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return ScheduledHeartbeat.NOOP_HEARTBEAT;
        }

        List<Heartbeat> heartbeats = heartbeatFactories
                .stream()
                .map(factory -> factory.getHeartbeat(
                        connectorConfig,
                        connectionProvider,
                        errorHandler,
                        queue))
                .flatMap(Optional::stream)
                .toList();

        return new CompositeHeartbeat(new HeartbeatImpl(
                connectorConfig.getHeartbeatInterval(),
                connectorConfig.getTopicNamingStrategy(TOPIC_NAMING_STRATEGY).heartbeatTopic(),
                connectorConfig.getLogicalName(),
                connectorConfig.schemaNameAdjuster(), queue), heartbeats);
    }

}
