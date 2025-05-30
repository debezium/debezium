/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.debezium.util.Threads;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

/**
 * Abstract base class for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public abstract class BinlogConnector<T extends BinlogConnectorConfig> extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnector.class);

    @Immutable
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        return Collections.singletonList(properties);
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final T connectorConfig = createConnectorConfig(config);
        Duration timeout = connectorConfig.getConnectionValidationTimeout();

        try {
            Threads.runWithTimeout(this.getClass(), () -> {
                try (BinlogConnectorConnection connection = createConnection(config, connectorConfig)) {
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
            }, timeout, connectorConfig.getLogicalName(), "connection-validation");
        }
        catch (TimeoutException e) {
            hostnameValue.addErrorMessage("Connection validation timed out after " + timeout.toMillis() + " ms");
        }
        catch (Exception e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TableId> getMatchingCollections(Configuration config) {
        final T connectorConfig = createConnectorConfig(config);
        try (BinlogConnectorConnection connection = createConnection(config, connectorConfig)) {
            final List<TableId> tables = new ArrayList<>();
            final List<String> databaseNames = connection.availableDatabases();
            final RelationalTableFilters tableFilter = connectorConfig.getTableFilters();
            for (String databaseName : databaseNames) {
                if (!tableFilter.databaseFilter().test(databaseName)) {
                    continue;
                }
                tables.addAll(
                        connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }).stream()
                                .filter(tableId -> tableFilter.dataCollectionFilter().isIncluded(tableId))
                                .collect(Collectors.toList()));
            }
            return tables;
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    /**
     * Create the connection.
     *
     * @param config the connector configuration; never null
     * @param connectorConfig the connector configuration; never null
     * @return the connector connection; never null
     */
    protected abstract BinlogConnectorConnection createConnection(Configuration config, T connectorConfig);

    /**
     * Create the connector configuration.
     *
     * @param config the configuration; never null
     * @return the connector-specific configuration
     */
    protected abstract T createConnectorConfig(Configuration config);
}
