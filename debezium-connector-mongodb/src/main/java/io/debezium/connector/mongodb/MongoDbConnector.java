/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;

/**
 * A Kafka Connect source connector that creates tasks that read the MongoDB change stream and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link io.debezium.connector.mongodb.MongoDbConnectorConfig}.
 */
public class MongoDbConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnector.class);

    private Configuration config;

    public MongoDbConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoDbConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        // Validate the configuration ...
        final Configuration config = Configuration.from(props);

        if (!config.validateAndRecord(MongoDbConnectorConfig.ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.config = config;
        LOGGER.info("Successfully started MongoDB connector");
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration available", maxTasks);
            return Collections.emptyList();
        }
        LOGGER.debug("Configuring MongoDB connector task");
        return List.of(config.asMap());
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping MongoDB connector");
        this.config = null;
        // Clear interrupt flag so the graceful termination is always attempted.
        Thread.interrupted();
        LOGGER.info("Stopped MongoDB connector");
    }

    @Override
    public ConfigDef config() {
        return MongoDbConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        final Configuration config = Configuration.from(connectorConfigs);

        // Validate all fields and get connection string validation result
        Map<String, ConfigValue> validation = validateAllFields(config);
        ConfigValue csValidation = validation.get(MongoDbConnectorConfig.CONNECTION_STRING.name());

        // Validate connection when connection string is otherwise valid
        if (csValidation.errorMessages().isEmpty()) {
            validateConnection(config, csValidation);
        }
        return new Config(new ArrayList<>(validation.values()));
    }

    public void validateConnection(Configuration config, ConfigValue connectionStringValidation) {
        ConnectionContext connectionContext = new ConnectionContext(config);

        try {
            // Check base connection by accessing first database name
            try (MongoClient client = connectionContext.getMongoClient()) {
                client.listDatabaseNames().first(); // only when we try to fetch results a connection gets established
            }

            // For RS clusters check that replica set name is present
            // Java driver is smart enough to work without it but the specs says it should be set
            if (!connectionContext.hasRequiredReplicaSetName()) {
                connectionStringValidation.addErrorMessage("Replica set not specified in connection string");
            }
        }
        catch (MongoException e) {
            connectionStringValidation.addErrorMessage("Unable to connect: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MongoDbConnectorConfig.EXPOSED_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CollectionId> getMatchingCollections(Configuration config) {
        try (MongoDbConnection connection = MongoDbConnection.create(config)) {
            return connection.collections();
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }
    }
}
