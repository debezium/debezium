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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection.MySqlConnectionConfiguration;
import io.debezium.util.Strings;

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
public class MySqlConnector extends SourceConnector {

    public static final String IMPLEMENTATION_PROP = "internal.implementation";
    public static final String LEGACY_IMPLEMENTATION = "legacy";

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
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = config.validate(MySqlConnectorConfig.EXPOSED_FIELDS);

        // Get the config values for each of the connection-related fields ...
        ConfigValue hostnameValue = results.get(MySqlConnectorConfig.HOSTNAME.name());
        ConfigValue portValue = results.get(MySqlConnectorConfig.PORT.name());
        ConfigValue userValue = results.get(MySqlConnectorConfig.USER.name());
        final String passwordValue = config.getString(MySqlConnectorConfig.PASSWORD);

        if (Strings.isNullOrEmpty(passwordValue)) {
            logger.warn("The connection password is empty");
        }

        // If there are no errors on any of these ...
        if (hostnameValue.errorMessages().isEmpty()
                && portValue.errorMessages().isEmpty()
                && userValue.errorMessages().isEmpty()) {
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
        return new Config(new ArrayList<>(results.values()));
    }
}
