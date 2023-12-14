/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.ConnectorAdapter;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * A Kafka Connect source connector that creates tasks that read the MySQL binary log and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MySqlConnectorConfig}.
 *
 *
 * @author Randall Hauch
 */
public class MySqlConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnector.class);

    @Immutable
    private Map<String, String> properties;

    public MySqlConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return io.debezium.connector.mysql.MySqlConnectorTask.class;
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
        return MySqlConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        ConfigValue adapterValue = configValues.get(MySqlConnectorConfig.CONNECTOR_ADAPTER.name());
        ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());

        ConnectorAdapter adapter = adapter(config);
        if (adapter == null) {
            LOGGER.error("Failed to resolve connection adapter.");
            adapterValue.addErrorMessage("Failed to resolve the connector's connection adapter.");
            return;
        }

        // Try to connect to the database ...
        try (AbstractConnectorConnection connection = adapter.createConnection(config)) {
            try {
                connection.connect();
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(), connection.connectionConfig().username());
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'", connection.connectionString(), connection.connectionConfig().username(), e);
                hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        catch (SQLException e) {
            LOGGER.error("Unexpected error shutting down the database connection", e);
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MySqlConnectorConfig.ALL_FIELDS);
    }

    private static ConnectorAdapter adapter(Configuration config) {
        // todo: find a better way to handle this
        return new MySqlConnectorConfig(config).getConnectorAdapter();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        final MySqlConnectorConfig mySqlConnectorConfig = new MySqlConnectorConfig(config);
        try (AbstractConnectorConnection connection = adapter(config).createConnection(config)) {
            final List<TableId> tables = new ArrayList<>();

            final List<String> databaseNames = connection.availableDatabases();

            for (String databaseName : databaseNames) {
                if (!mySqlConnectorConfig.getTableFilters().databaseFilter().test(databaseName)) {
                    continue;
                }
                tables.addAll(connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }));
            }
            return tables;
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }
}
