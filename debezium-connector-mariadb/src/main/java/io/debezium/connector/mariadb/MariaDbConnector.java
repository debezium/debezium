/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.mariadb.jdbc.MariaDbConnection;
import io.debezium.connector.mariadb.jdbc.MariaDbConnectionConfiguration;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * A Debezium source connector that creates tasks and reads changes from MariaDB's binary transaction logs,
 * generating change events.
 *
 * @author Chris Cranford
 */
public class MariaDbConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnector.class);

    @Immutable
    private Map<String, String> properties;

    public MariaDbConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MariaDbConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return MariaDbConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        try (MariaDbConnection connection = createConnection(config)) {
            try {
                connection.connect();
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'",
                        connection.connectionString(), connection.connectionConfig().username());
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'",
                        connection.connectionString(), connection.connectionConfig().username(), e);
                hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        catch (SQLException e) {
            LOGGER.error("Unexpected error shutting down the database connection", e);
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MariaDbConnectorConfig.ALL_FIELDS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TableId> getMatchingCollections(Configuration config) {
        final MariaDbConnectorConfig connectorConfig = createConnectorConfig(config);
        try (MariaDbConnection connection = createConnection(config)) {
            final List<TableId> tables = new ArrayList<>();

            final List<String> databaseNames = connection.availableDatabases();

            for (String databaseName : databaseNames) {
                if (!connectorConfig.getTableFilters().databaseFilter().test(databaseName)) {
                    continue;
                }
                tables.addAll(
                        connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }).stream()
                                .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                                .collect(Collectors.toList()));
            }
            return tables;
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    private MariaDbConnectorConfig createConnectorConfig(Configuration config) {
        return new MariaDbConnectorConfig(config);
    }

    private MariaDbConnection createConnection(Configuration config) {
        final MariaDbConnectionConfiguration connectionConfig = new MariaDbConnectionConfiguration(config);
        return new MariaDbConnection(connectionConfig, new MariaDbFieldReader(createConnectorConfig(config)));
    }
}
