/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * A Kafka Connect source connector that creates tasks which use Postgresql streaming replication off a logical replication slot
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link PostgresConnectorConfig}.
 *
 * @author Horia Chiorean
 */
public class PostgresConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnector.class);
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
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        final ConfigValue slotNameValue = configValues.get(PostgresConnectorConfig.SLOT_NAME.name());
        final ConfigValue pluginNameValue = configValues.get(PostgresConnectorConfig.PLUGIN_NAME.name());
        if (!databaseValue.errorMessages().isEmpty() || !slotNameValue.errorMessages().isEmpty()
                || !pluginNameValue.errorMessages().isEmpty()) {
            return;
        }

        final PostgresConnectorConfig postgresConfig = new PostgresConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue portValue = configValues.get(PostgresConnectorConfig.PORT.name());
        final ConfigValue userValue = configValues.get(PostgresConnectorConfig.USER.name());
        final ConfigValue passwordValue = configValues.get(PostgresConnectorConfig.PASSWORD.name());
        final ConfigValue logicalValue = configValues.get(PostgresConnectorConfig.SERVER_NAME.name());

        // Try to connect to the database ...
        try (PostgresConnection connection = new PostgresConnection(postgresConfig.getJdbcConfig(), PostgresConnection.CONNECTION_VALIDATE_CONNECTION)) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                // check connection
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                        connection.username());
                // check server wal_level
                final String walLevel = connection.queryAndMap(
                        "SHOW wal_level",
                        connection.singleResultMapper(rs -> rs.getString("wal_level"), "Could not fetch wal_level"));
                if (!"logical".equals(walLevel)) {
                    final String errorMessage = "Postgres server wal_level property must be \"logical\" but is: " + walLevel;
                    LOGGER.error(errorMessage);
                    hostnameValue.addErrorMessage(errorMessage);
                }
                // check user for LOGIN and REPLICATION roles
                if (!connection.queryAndMap(
                        "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication," +
                        // for AWS the user might not have directly the rolreplication rights, but can be assigned
                        // to one of those role groups: rds_superuser, rdsadmin or rdsrepladmin
                                " CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser" +
                                ", CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin" +
                                ", CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin" +
                                ", CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rds_replication') AS BOOL) IS TRUE AS aws_replication" +
                                " FROM pg_roles r WHERE r.rolname = current_user",
                        connection.singleResultMapper(rs -> rs.getBoolean("rolcanlogin")
                                && (rs.getBoolean("rolreplication")
                                        || rs.getBoolean("aws_superuser")
                                        || rs.getBoolean("aws_admin")
                                        || rs.getBoolean("aws_repladmin")
                                        || rs.getBoolean("aws_replication")),
                                "Could not fetch roles"))) {
                    final String errorMessage = "Postgres roles LOGIN and REPLICATION are not assigned to user: " + connection.username();
                    LOGGER.error(errorMessage);
                }

                // check for DB version and LogicalDecoder compatibility
                if (LogicalDecoder.PGOUTPUT.equals(postgresConfig.plugin())) {
                    final Version dbVersion = ServerVersion.from(
                            connection.queryAndMap(
                                    "SHOW server_version",
                                    connection.singleResultMapper(
                                            rs -> rs.getString("server_version"),
                                            "Could not fetch db version")));
                    if (ServerVersion.v10.getVersionNum() > dbVersion.getVersionNum()) {
                        final String errorMessage = "PGOUTPUT plugin is only supported on "
                                + "postgres server version 10+";
                        LOGGER.error(errorMessage);
                        hostnameValue.addErrorMessage(errorMessage);
                        pluginNameValue.addErrorMessage(errorMessage);
                    }
                }
            }
            catch (Exception e) {
                LOGGER.error("Failed testing connection for {} with user '{}'", connection.connectionString(),
                        connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                portValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                databaseValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                userValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                passwordValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                logicalValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
            }
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(PostgresConnectorConfig.ALL_FIELDS);
    }
}
