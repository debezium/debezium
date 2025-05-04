/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.ConnectionValidationUtil;
import io.debezium.util.Strings;

public class OracleConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnector.class);

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
        return OracleConnectorTask.class;
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
        return OracleConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue userValue = configValues.get(RelationalDatabaseConnectorConfig.USER.name());

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        long timeoutMs = connectorConfig.getConnectionValidationTimeoutMs();

        try {
            ConnectionValidationUtil.runWithTimeout(OracleConnector.class, () -> {
                try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig())) {
                    LOGGER.debug("Successfully tested connection for {} with user '{}'", OracleConnection.connectionString(connectorConfig.getJdbcConfig()),
                            connection.username());
                }
                catch (SQLException | RuntimeException e) {
                    LOGGER.error("Failed testing connection for {} with user '{}'", config.withMaskedPasswords(), userValue, e);
                    hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
                }
            }, timeoutMs, connectorConfig.getLogicalName());
        }
        catch (TimeoutException e) {
            hostnameValue.addErrorMessage("Connection validation timed out after " + timeoutMs + " ms");
        }
        catch (Exception e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(OracleConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final String databaseName = connectorConfig.getCatalogName();

        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), false)) {
            if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
                connection.setSessionToPdb(connectorConfig.getPdbName());
            }
            // @TODO: we need to expose a better method from the connector, particularly getAllTableIds
            // the following's performance is acceptable when using PDBs but not as ideal with non-PDB
            return connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }).stream()
                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }
}
