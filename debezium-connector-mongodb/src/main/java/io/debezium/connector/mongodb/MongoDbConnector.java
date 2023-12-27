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
import io.debezium.connector.mongodb.connection.ConnectionStrings;
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
    private ConnectionContext connectionContext;

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
        this.connectionContext = new ConnectionContext(config);

        LOGGER.info("Successfully started MongoDB connector, and continuing to discover at {}", connectionContext.getMaskedConnectionString());
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration available", maxTasks);
            return Collections.emptyList();
        }

        // ensure connection string has replicaSet when possible
        var taskConnectionString = connectionContext.resolveTaskConnectionString();
        LOGGER.info("Configuring MongoDB connector task to capture events for connections to: {}", ConnectionStrings.mask(taskConnectionString));

        var taskConfig = config.edit()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, taskConnectionString)
                .with(MongoDbConnectorConfig.TASK_ID, 0)
                .build()
                .asMap();

        LOGGER.debug("Configuring MongoDB connector task");
        return List.of(taskConfig);
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

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);

        // Get the config values for each of the connection-related fields ...
        ConfigValue connectionStringValue = results.get(MongoDbConnectorConfig.CONNECTION_STRING.name());
        ConfigValue userValue = results.get(MongoDbConnectorConfig.USER.name());
        ConfigValue passwordValue = results.get(MongoDbConnectorConfig.PASSWORD.name());

        // If there are no errors on any of these ...
        if (userValue.errorMessages().isEmpty()
                && passwordValue.errorMessages().isEmpty()
                && connectionStringValue.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            ConnectionContext connContext = new ConnectionContext(config);
            try (MongoClient client = connContext.getMongoClient()) {
                client.listDatabaseNames().first(); // only when we try to fetch results a connection gets established
            }
            catch (MongoException e) {
                connectionStringValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        return new Config(new ArrayList<>(results.values()));
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
