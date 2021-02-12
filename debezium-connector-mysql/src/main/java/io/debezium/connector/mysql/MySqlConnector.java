/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.mysql.MySqlConnection.MySqlConnectionConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

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

    public static final String IMPLEMENTATION_PROP = "internal.implementation";
    private static final String LEGACY_IMPLEMENTATION = "legacy";

    private Logger logger = LoggerFactory.getLogger(getClass());
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
        final String implementation = properties.get(IMPLEMENTATION_PROP);
        if (isLegacy(implementation)) {
            logger.warn("Legacy MySQL connector implementation is enabled");
            return io.debezium.connector.mysql.legacy.MySqlConnectorTask.class;
        }
        return io.debezium.connector.mysql.MySqlConnectorTask.class;
    }

    static boolean isLegacy(final String implementation) {
        return LEGACY_IMPLEMENTATION.equals(implementation);
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
        ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        final MySqlConnectionConfiguration connectionConfig = new MySqlConnectionConfiguration(config);
        try (MySqlConnection connection = new MySqlConnection(connectionConfig)) {
            try {
                connection.connect();
                connection.execute("SELECT version()");
                logger.info("Successfully tested connection for {} with user '{}'", connection.connectionString(), connectionConfig.username());
            }
            catch (SQLException e) {
                logger.info("Failed testing connection for {} with user '{}'", connection.connectionString(), connectionConfig.username());
                hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        catch (SQLException e) {
            logger.error("Unexpected error shutting down the database connection", e);
        }
    }
}
