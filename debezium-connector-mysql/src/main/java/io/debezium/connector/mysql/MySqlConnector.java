/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.datapipeline.base.connector.config.DpTaskConfig;
import com.datapipeline.base.connector.source.DpSourceConnector;
import com.datapipeline.base.connector.topic.DpTopicMetaStorage;
import com.datapipeline.base.connector.topic.TopicNameFormatter;
import com.datapipeline.base.utils.JsonConvert;
import com.datapipeline.clients.DpEnv;
import com.dp.internal.bean.DataPipelineActiveBean;
import com.dp.internal.bean.DataSourceBean;
import com.dp.internal.bean.DataSourceWhiteListBean;
import com.dp.internal.bean.DpSqlConnectInfoBean;

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
    protected void buildConfigMap(Map<String, String> config, DpTaskConfig dpTaskConfig) {
        DataPipelineActiveBean activeBean = dpTaskConfig.getDataPipelineActiveBean();
        DataSourceBean dataSourceBean =
                JsonConvert.getObject(activeBean.getDataSource(), DataSourceBean.class);
        DpSqlConnectInfoBean connectInfo =
                JsonConvert.getObject(dataSourceBean.getConfig(), DpSqlConnectInfoBean.class);

        String dpTaskId = dpTaskConfig.getDpTaskId();
        String serverName = TopicNameFormatter.getNameBody(dpTaskId);
        String schemaChangeTopicName = TopicNameFormatter.getSchemaChangeTopicName(dpTaskId);

        config.put("database.hostname", connectInfo.getSqlHostname());
        config.put("database.port", String.valueOf(connectInfo.getSqlPort()));
        config.put("database.user", connectInfo.getSqlUsername());
        config.put("database.password", connectInfo.getSqlPassword());
        config.put("snapshot.mode", "when_needed");
        config.put("database.whitelist", connectInfo.getSqlDatabase());
        if (dataSourceBean.getTableWhiteLists() != null && !dataSourceBean.getTableWhiteLists().isEmpty()) {
            List<String> tableSchemalist = new ArrayList<>();
            List<String> tablewhitelist = new ArrayList<>();
            for (DataSourceWhiteListBean beans : dataSourceBean.getTableWhiteLists()) {
                if (isNotBlank(beans.getSchemaNames())) {
                    tableSchemalist.add(
                            String.format("%s:[%s]", beans.getSchemaNames(), String.join(",", beans.getTables())));
                }
                for (String tableName : beans.getTables()) {
                    tablewhitelist.add(String.format("%s.%s", connectInfo.getSqlDatabase(), tableName));
                }
            }
            config.put("table.whitelist", String.join(",", tablewhitelist));
            config.put("table.schema.map", String.join(",", tableSchemalist));
        }
        config.put("topic.generation.mode", "merge");
        config.put("database.server.name", serverName);
        config.put(
                "database.history.kafka.bootstrap.servers",
                new DpEnv().getString(DpEnv.KAFKA_CLUSTER_ADDRESSES, "172.17.0.1:9092"));
        config.put("database.history.kafka.topic", schemaChangeTopicName);

        String namespace = connectInfo.getSqlDatabase();
        dpTaskConfig.getDpSchemaList().forEach(dpSchemaName ->
                DpTopicMetaStorage.INSTANCE.register(dpTaskId, namespace, dpSchemaName));
    }

    @Override
    public ConfigDef config() {
        return MySqlConnectorConfig.baseConfigDef();
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
