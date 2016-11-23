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
import io.debezium.jdbc.JdbcConnection;

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
    
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;

    public MySqlConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySqlConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<String, String>(props));
    }

    @Override
    public void stop() {
        this.props = null;
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
        ConfigValue passwordValue = results.get(MySqlConnectorConfig.PASSWORD.name());

        // If there are no errors on any of these ...
        if (hostnameValue.errorMessages().isEmpty()
                && portValue.errorMessages().isEmpty()
                && userValue.errorMessages().isEmpty()
                && passwordValue.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            try (MySqlJdbcContext jdbcContext = new MySqlJdbcContext(config)) {
                jdbcContext.start();
                JdbcConnection mysql = jdbcContext.jdbc();
                try {
                    mysql.execute("SELECT version()");
                    logger.info("Successfully tested connection for {} with user '{}'", jdbcContext.connectionString(), mysql.username());
                } catch (SQLException e) {
                    logger.info("Failed testing connection for {} with user '{}'", jdbcContext.connectionString(), mysql.username());
                    hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
                } finally {
                    jdbcContext.shutdown();
                }
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }
}
