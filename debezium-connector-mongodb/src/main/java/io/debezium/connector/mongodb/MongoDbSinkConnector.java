/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
import io.debezium.connector.mongodb.sink.Module;
import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorTask;
import io.debezium.metadata.ConfigDescriptor;

public class MongoDbSinkConnector extends SinkConnector implements ConfigDescriptor {

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
    public Field.Set getConfigFields() {
        return MongoDbSinkConnectorConfig.ALL_FIELDS;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);

        MongoDbSinkConnectorConfig sinkConfig;
        try {
            sinkConfig = new MongoDbSinkConnectorConfig(Configuration.from(connectorConfigs));
            sinkConfig.validate();
        }
        catch (Exception e) {
            return config;
        }

        final var connectionStringValidation = config.configValues().stream()
                .filter(value -> value.name().equals(MongoDbSinkConnectorConfig.CONNECTION_STRING.name()))
                .findFirst().orElseThrow();
        if (connectionStringValidation.errorMessages().isEmpty()) {
            try (var connectionContext = new MongoDbConnectionContext(Configuration.from(connectorConfigs))) {
                if (!connectionContext.canConnect()) {
                    connectionStringValidation.addErrorMessage("Unable to connect to the server.");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException(e);
            }
            catch (RuntimeException e) {
                connectionStringValidation.addErrorMessage("Error during connection validation: " + e.getMessage());
            }
        }

        return config;
    }
}
