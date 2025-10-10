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
public class MongoDbTaskContext extends CdcSourceTaskContext<MongoDbConnectorConfig> {

    private final Filters filters;
    private final String serverName;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super(config,
                new MongoDbConnectorConfig(config),
                config.getString(MongoDbConnectorConfig.TASK_ID),
                new MongoDbConnectorConfig(config).getCustomMetricTags());

        this.filters = new Filters(config);
        this.serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
    }

    public Filters getFilters() {
        return filters;
    }

    public String getServerName() {
        return serverName;
    }
}
