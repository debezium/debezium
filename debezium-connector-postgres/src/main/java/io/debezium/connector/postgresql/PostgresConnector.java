/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

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
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Strings;

/**
 * A Kafka Connect source connector that creates tasks which use Postgresql streaming replication off a logical replication slot
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link PostgresConnectorConfig}.
 *
 * @author Horia Chiorean
 */
public class PostgresConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;

    public PostgresConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // this will always have just one task with the given list of properties
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
    }

    @Override
    public void stop() {
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return PostgresConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        PostgresConnectorConfig config = new PostgresConnectorConfig(Configuration.from(connectorConfigs));

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = config.validate();

        // Get the config values for each of the connection-related fields ...
        ConfigValue hostnameValue = results.get(PostgresConnectorConfig.HOSTNAME.name());
        ConfigValue portValue = results.get(PostgresConnectorConfig.PORT.name());
        ConfigValue databaseValue = results.get(PostgresConnectorConfig.DATABASE_NAME.name());
        ConfigValue userValue = results.get(PostgresConnectorConfig.USER.name());
        ConfigValue passwordValue = results.get(PostgresConnectorConfig.PASSWORD.name());
        final String passwordStringValue = config.getConfig().getString(PostgresConnectorConfig.PASSWORD);

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
            try (PostgresConnection connection = new PostgresConnection(config.jdbcConfig())) {
                try {
                    connection.execute("SELECT version()");
                    logger.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                            connection.username());
                }
                catch (SQLException e) {
                    logger.info("Failed testing connection for {} with user '{}'", connection.connectionString(),
                            connection.username());
                    hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
                }
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }
}
