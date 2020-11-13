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
        final PostgresConnectorConfig config = new PostgresConnectorConfig(Configuration.from(connectorConfigs));

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        final Map<String, ConfigValue> results = config.validate();

        // Get the config values for each of the connection-related fields ...
        final ConfigValue hostnameResult = results.get(PostgresConnectorConfig.HOSTNAME.name());
        final ConfigValue portResult = results.get(PostgresConnectorConfig.PORT.name());
        final ConfigValue databaseNameResult = results.get(PostgresConnectorConfig.DATABASE_NAME.name());
        final ConfigValue userResult = results.get(PostgresConnectorConfig.USER.name());
        final ConfigValue passwordResult = results.get(PostgresConnectorConfig.PASSWORD.name());
        final ConfigValue slotNameResult = results.get(PostgresConnectorConfig.SLOT_NAME.name());
        final ConfigValue pluginNameResult = results.get(PostgresConnectorConfig.PLUGIN_NAME.name());
        final String passwordStringValue = config.getConfig().getString(PostgresConnectorConfig.PASSWORD);

        if (Strings.isNullOrEmpty(passwordStringValue)) {
            logger.warn("The connection password is empty");
        }

        // If there are no errors on any of these ...
        if (hostnameResult.errorMessages().isEmpty()
                && portResult.errorMessages().isEmpty()
                && userResult.errorMessages().isEmpty()
                && passwordResult.errorMessages().isEmpty()
                && databaseNameResult.errorMessages().isEmpty()
                && slotNameResult.errorMessages().isEmpty()
                && pluginNameResult.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            try (PostgresConnection connection = new PostgresConnection(config.jdbcConfig())) {
                try {
                    // check connection
                    connection.execute("SELECT version()");
                    logger.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                            connection.username());
                    // check server wal_level
                    final String walLevel = connection.queryAndMap(
                            "SHOW wal_level",
                            connection.singleResultMapper(rs -> rs.getString("wal_level"), "Could not fetch wal_level"));
                    if (!"logical".equals(walLevel)) {
                        final String errorMessage = "Postgres server wal_level property must be \"logical\" but is: " + walLevel;
                        logger.error(errorMessage);
                        hostnameResult.addErrorMessage(errorMessage);
                    }
                    // check user for LOGIN and REPLICATION roles
                    if (!connection.queryAndMap(
                            "SELECT rolcanlogin, rolreplication FROM pg_roles WHERE rolname = current_user",
                            connection.singleResultMapper(rs -> rs.getBoolean("rolcanlogin") && rs.getBoolean("rolreplication"), "Could not fetch roles"))) {
                        final String errorMessage = "Postgres roles LOGIN and REPLICATION are not assigned to user: " + connection.username();
                        logger.error(errorMessage);
                        userResult.addErrorMessage(errorMessage);
                    }
                    // check replication slot
                    final String slotName = config.slotName();
                    if (connection.prepareQueryAndMap(
                            "SELECT * FROM pg_replication_slots WHERE slot_name = ?",
                            statement -> statement.setString(1, slotName),
                            rs -> {
                                if (rs.next()) {
                                    return rs.getBoolean("active");
                                }
                                return false;
                            })) {
                        final String errorMessage = "Slot name \"" + slotName
                                + "\" already exists and is active. Choose a unique name or stop the other process occupying the slot.";
                        logger.error(errorMessage);
                        slotNameResult.addErrorMessage(errorMessage);
                    }
                }
                catch (SQLException e) {
                    logger.error("Failed testing connection for {} with user '{}': {}", connection.connectionString(),
                            connection.username(), e.getMessage());
                    hostnameResult.addErrorMessage("Error while validating connector config: " + e.getMessage());
                }
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }
}
