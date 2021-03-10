/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

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
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Clock;

/**
 * The main connector class used to instantiate configuration and execution classes
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnector.class);

    private Map<String, String> properties;

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
        return SqlServerConnectorTask.class;
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
        return SqlServerConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue userValue = configValues.get(RelationalDatabaseConnectorConfig.USER.name());
        SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(config);
        // Try to connect to the database ...
        try (SqlServerConnection connection = new SqlServerConnection(sqlServerConfig.jdbcConfig(), Clock.system(),
                sqlServerConfig.getSourceTimestampMode(), null)) {
            connection.test();
            LOGGER.debug("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                    connection.username());
        }
        catch (Exception e) {
            LOGGER.error("Failed testing connection for {} with user '{}'", config.withMaskedPasswords(),
                    userValue, e);
            hostnameValue.addErrorMessage("Unable to connect. Check this and other connection properties. Error: "
                    + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(SqlServerConnectorConfig.ALL_FIELDS);
    }
}
