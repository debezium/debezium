/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.datapipeline.base.connector.config.DpTaskConfig;
import com.datapipeline.base.connector.source.DpSourceConnector;
import com.datapipeline.base.connector.task.config.MysqlSourceTaskConfig;
import com.datapipeline.clients.DpEnv;
import com.datapipeline.clients.TopicNameFormatter;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

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
public class MySqlConnector extends DpSourceConnector {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Class<? extends Task> taskClass() {
        return MySqlConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> props = getConfigProps();
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
    }

    @Override
    protected Map<String, String> getConfigMap(Map<String, String> props) {
        MysqlSourceTaskConfig sourceTaskConfig =
                DpTaskConfig.fromConnectorProps(props, MysqlSourceTaskConfig.class);
        Map<String, String> config = new HashMap<>(props);

        String dpTaskId = sourceTaskConfig.getDpTaskId();
        String serverName = TopicNameFormatter.getNameBody(dpTaskId);
        String schemaChangeTopicName = TopicNameFormatter.getSchemaChangeTopicName(dpTaskId);

        config.put("database.hostname", sourceTaskConfig.getHostName());
        config.put("database.port", sourceTaskConfig.getPort());
        config.put("database.user", sourceTaskConfig.getUserName());
        config.put("database.password", sourceTaskConfig.getPassword());
        config.put("snapshot.mode", "when_needed");
        config.put("database.whitelist", sourceTaskConfig.getDbName());
        if (isNotBlank(sourceTaskConfig.getTableWhitelist())) {
            config.put("table.whitelist", sourceTaskConfig.getTableWhitelist());
        }
        if (isNotBlank(sourceTaskConfig.getTableSchemaMap())) {
            config.put("table.schema.map", sourceTaskConfig.getTableSchemaMap());
        }
        config.put("topic.generation.mode", "merge");
        config.put("database.server.name", serverName);
        config.put(
                "database.history.kafka.bootstrap.servers",
                new DpEnv().getString(DpEnv.KAFKA_CLUSTER_ADDRESSES, "172.17.0.1:9092"));
        config.put("database.history.kafka.topic", schemaChangeTopicName);

        return config;
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
