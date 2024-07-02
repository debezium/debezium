/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;

public class MongoDbSinkConnector extends SinkConnector {

    @Immutable
    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoDbSinkConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(properties);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return MongoDbSinkConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);

        MongoDbSinkConnectorConfig sinkConfig;
        try {
            sinkConfig = new MongoDbSinkConnectorConfig(Configuration.from(connectorConfigs));
        }
        catch (Exception e) {
            return config;
        }

        SinkConnection.canConnect(config, MongoDbSinkConnectorConfig.CONNECTION_STRING);

        return config;
    }
}
