/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.util.Testing;

public class MongoDbTaskContextTest implements Testing {
    private Configuration config;
    private MongoDbTaskContext context;

    @Before
    public void setup() {
        this.config = Configuration.create()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://dummy:27017")
                .with(MongoDbConnectorConfig.TASK_ID, 42)
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "bistromath")
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS)
                .build();
        this.context = new MongoDbTaskContext(config);
    }

    @Test
    public void shouldConfigureCommonTaskPropertiesFromConfig() {
        assertThat(context.getTaskId()).isEqualTo(config.getString(MongoDbConnectorConfig.TASK_ID));
        assertThat(context.getConnectorLogicalName()).isEqualTo(config.getString(CommonConnectorConfig.TOPIC_PREFIX));
        assertThat(context.getConnectorType()).isEqualTo(Module.contextName());
    }

    @Test
    public void shouldConfigureMongoDbTaskPropertiesFromConfig() {
        assertThat(context.getConnectorConfig().getConfig()).isEqualTo(config);
        assertThat(context.getConnectorConfig().getCaptureMode()).isEqualTo(MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS);
    }
}
