/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.schema.TopicSelector;

/**
 * @author Randall Hauch
 */
public class MongoDbTaskContext extends CdcSourceTaskContext {

    private final Filters filters;
    private final TopicSelector<CollectionId> topicSelector;
    private final SourceInfo source;
    private final String serverName;
    private final ConnectionContext connectionContext;
    private final MongoDbConnectorConfig connectorConfig;
    private CaptureMode captureMode;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super(Module.contextName(),
                config.getString(MongoDbConnectorConfig.LOGICAL_NAME),
                String.valueOf(config.getInteger(MongoDbConnectorConfig.TASK_ID, 0)),
                Collections::emptySet);

        final String serverName = config.getString(MongoDbConnectorConfig.LOGICAL_NAME);
        this.filters = new Filters(config);
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.topicSelector = MongoDbTopicSelector.defaultSelector(serverName, connectorConfig.getHeartbeatTopicsPrefix());
        this.serverName = config.getString(MongoDbConnectorConfig.LOGICAL_NAME);
        this.connectionContext = new ConnectionContext(config);
        this.overrideCaptureMode(connectorConfig.getCaptureMode());
        this.source = new SourceInfo(connectorConfig, this.getMongoTaskId());
    }

    public TopicSelector<CollectionId> topicSelector() {
        return topicSelector;
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
     * configured value as the offsets stored might be created by a different capture mode.
     * In this case the configured value is overriden and the mode previously used is restored.
     *
     * @return effectively used capture mode
     */
    public CaptureMode getCaptureMode() {
        return captureMode;
    }

    public void overrideCaptureMode(CaptureMode captureModeUsed) {
        this.captureMode = captureModeUsed;
    }

    public int getMongoTaskId() {
        return Integer.parseInt(getTaskId());
    }
}
