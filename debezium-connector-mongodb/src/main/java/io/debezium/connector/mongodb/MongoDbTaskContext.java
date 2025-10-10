/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * @author Randall Hauch
 */
public class MongoDbTaskContext extends CdcSourceTaskContext {

    private final Filters filters;
    private final String serverName;
    private final MongoDbConnectorConfig connectorConfig;
    private final Configuration config;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super(new MongoDbConnectorConfig(config),
                config.getString(MongoDbConnectorConfig.TASK_ID),
                new MongoDbConnectorConfig(config).getCustomMetricTags());

        this.filters = new Filters(config);
        this.config = config;
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
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

    public Configuration getConfiguration() {
        return this.config;
    }
}
