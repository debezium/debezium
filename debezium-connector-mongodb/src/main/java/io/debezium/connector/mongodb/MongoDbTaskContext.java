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
import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * @author Randall Hauch
 */
public class MongoDbTaskContext extends CdcSourceTaskContext {

    private final Filters filters;
    private final SourceInfo source;
    private final TopicNamingStrategy topicNamingStrategy;
    private final String serverName;
    private final ConnectionContext connectionContext;
    private final MongoDbConnectorConfig connectorConfig;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super(Module.contextName(), config.getString(CommonConnectorConfig.TOPIC_PREFIX), config.getString(MongoDbConnectorConfig.TASK_ID), Collections::emptySet);

        this.filters = new Filters(config);
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.source = new SourceInfo(connectorConfig);
        this.topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MongoDbConnectorConfig.TOPIC_NAMING_STRATEGY);
        this.serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
        this.connectionContext = new ConnectionContext(config);
    }

    public TopicNamingStrategy<CollectionId> topicNamingStrategy() {
        return topicNamingStrategy;
    }

    public Filters filters() {
        return filters;
    }

    public SourceInfo source() {
        return source;
    }

    public String serverName() {
        return serverName;
    }

    public ConnectionContext getConnectionContext() {
        return connectionContext;
    }

    public MongoDbConnectorConfig getConnectorConfig() {
        return this.connectorConfig;
    }

    /**
     * Provides the capture mode used by connector runtime. This value can differ from requested
     * configured value as the offets stored might be created by a different capture mode.
     * In this case the configured value is overriden and the mode previously used is restored.
     *
     * @return effectively used capture mode
     */
    public CaptureMode getCaptureMode() {
        return connectorConfig.getCaptureMode();
    }
}
