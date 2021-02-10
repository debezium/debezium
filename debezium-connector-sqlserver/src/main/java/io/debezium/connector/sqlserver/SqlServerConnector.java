/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

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
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * The main connector class used to instantiate configuration and execution classes
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(getClass());

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
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);
        SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(config);
        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = config.validate(SqlServerConnectorConfig.ALL_FIELDS);

        // Get the config values for each of the connection-related fields ...
        ConfigValue hostnameValue = results.get(SqlServerConnectorConfig.HOSTNAME.name());
        ConfigValue portValue = results.get(SqlServerConnectorConfig.PORT.name());
        ConfigValue databaseValue = results.get(SqlServerConnectorConfig.DATABASE_NAME.name());
        ConfigValue userValue = results.get(SqlServerConnectorConfig.USER.name());
        ConfigValue passwordValue = results.get(SqlServerConnectorConfig.PASSWORD.name());
        final String passwordStringValue = config.getString(SqlServerConnectorConfig.PASSWORD);

        if (Strings.isNullOrEmpty(passwordStringValue)) {
            logger.warn("The connection password is empty");
        }

        // If there are no errors on any of these ...
        if (hostnameValue.errorMessages().isEmpty()
                && portValue.errorMessages().isEmpty()
                && userValue.errorMessages().isEmpty()
                && passwordValue.errorMessages().isEmpty()
                && databaseValue.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            try (SqlServerConnection connection = new SqlServerConnection(sqlServerConfig.jdbcConfig(), Clock.system(),
                    sqlServerConfig.getSourceTimestampMode(), null)) {
                // SqlServerConnection will try retrieving database, no need to run another query.
                logger.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                        connection.username());
            }
            catch (Throwable e) {
                logger.info("Failed testing connection for {} with user '{}'", sqlServerConfig.jdbcConfig(),
                        userValue);
                hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
                portValue.addErrorMessage("Unable to connect: " + e.getMessage());
                databaseValue.addErrorMessage("Unable to connect: " + e.getMessage());
                userValue.addErrorMessage("Unable to connect: " + e.getMessage());
                passwordValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }
}
