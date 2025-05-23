/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

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
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
import io.debezium.connector.mongodb.connection.MongoDbConnections;
import io.debezium.util.Threads;

/**
 * A Kafka Connect source connector that creates tasks that read the MongoDB change stream and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link io.debezium.connector.mongodb.MongoDbConnectorConfig}.
 */
public class MongoDbConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnector.class);
    public static final String DEPRECATED_SHARD_CS_PARAMS_FILED = "mongodb.connection.string.shard.params";
    public static final String DEPRECATED_CONNECTION_MODE_FILED = "mongodb.connection.mode";

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
        // Shard specific parameters shouldn't be set after RS connection mode removal
        if (config.hasKey(DEPRECATED_SHARD_CS_PARAMS_FILED)) {
            LOGGER.warn("Field '{}' is deprecated. Use only '{}' to set connection parameters", DEPRECATED_SHARD_CS_PARAMS_FILED,
                    MongoDbConnectorConfig.CONNECTION_STRING.name());
            connectionStringValidation.addErrorMessage("Deprecated field '" + DEPRECATED_SHARD_CS_PARAMS_FILED + "' is used");
        }

        // RS connection mode should not be used
        var mode = config.getString(DEPRECATED_CONNECTION_MODE_FILED);
        if (mode != null && mode.equals("replica_set")) {
            LOGGER.warn("Field '{}' is deprecated. Sharded mode is now used implicitly, please remove it. ", DEPRECATED_CONNECTION_MODE_FILED);
            connectionStringValidation.addErrorMessage("Deprecated field '" + DEPRECATED_CONNECTION_MODE_FILED + "' is used set to removed 'replica_set' value");
        }

        MongoDbConnectionContext connectionContext = new MongoDbConnectionContext(config);
        MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);
        Duration timeout = connectorConfig.getConnectionValidationTimeout();

        try {
            Threads.runWithTimeout(MongoDbConnector.class, () -> {
                try {
                    // Check base connection by accessing first database name
                    try (MongoClient client = connectionContext.getMongoClient()) {
                        client.listDatabaseNames().first(); // only when we try to fetch results a connection gets established
                    }

                    // For RS clusters check that replica set name is present
                    // Java driver is smart enough to work without it but the specs says it should be set
                    if (!connectionContext.hasReplicaSetNameIfRequired()) {
                        var type = connectionContext.getClusterType();
                        LOGGER.warn("Replica set not specified in connection string for {} cluster.", type);
                        connectionStringValidation.addErrorMessage("Replica set not specified in connection string for " + type + " cluster.");
                    }
                }
                catch (MongoException e) {
                    connectionStringValidation.addErrorMessage("Unable to connect: " + e.getMessage());
                }
            }, timeout, connectorConfig.getLogicalName(), "connection-validation", connectorConfig.connectorName(),
             connectorConfig.getConnectorThreadNamePattern(), connectorConfig.getTaskId());
        }
        catch (TimeoutException e) {
            connectionStringValidation.addErrorMessage("Connection validation timed out after " + timeout.toMillis() + "ms");
        }
        catch (Exception e) {
            connectionStringValidation.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MongoDbConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CollectionId> getMatchingCollections(Configuration config) {
        try (MongoDbConnection connection = MongoDbConnections.create(config)) {
            return connection.collections();
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }
    }
}
