/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.MongoDbConnections;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * @author Randall Hauch
 */
public class MongoDbTaskContext extends CdcSourceTaskContext {

    private final Filters filters;
    private final TopicNamingStrategy topicNamingStrategy;
    private final String serverName;
    private final MongoDbConnectorConfig connectorConfig;
    private final Configuration config;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super(new MongoDbConnectorConfig(config),
                config.getString(MongoDbConnectorConfig.TASK_ID),
                new MongoDbConnectorConfig(config).getCustomMetricTags(),
                Collections::emptySet);

        this.filters = new Filters(config);
        this.config = config;
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MongoDbConnectorConfig.TOPIC_NAMING_STRATEGY);
        this.serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
    }

    public TopicNamingStrategy<CollectionId> getTopicNamingStrategy() {
        return topicNamingStrategy;
    }

    public Filters getFilters() {
        return filters;
    }

    public String getServerName() {
        return serverName;
    }

    public MongoDbConnectorConfig getConnectorConfig() {
        return this.connectorConfig;
    }

    /**
     * Provides the capture mode used by connector runtime. This value can differ from requested
     * configured value as the offsets stored might be created by a different capture mode.
     * In this case the configured value is overridden and the mode previously used is restored.
     *
     * @return effectively used capture mode
     */
    public CaptureMode getCaptureMode() {
        return connectorConfig.getCaptureMode();
    }

    /**
     * Obtains instances of {@link MongoDbConnection} which should be used in event sources
     *
     * @param dispatcher event dispatcher
     * @param partition MongoDB partition
     * @return instance of {@link MongoDbConnection}
     */
    public MongoDbConnection getConnection(EventDispatcher<MongoDbPartition, CollectionId> dispatcher, MongoDbPartition partition) {
        return MongoDbConnections.create(config, dispatcher, partition);
    }
}
