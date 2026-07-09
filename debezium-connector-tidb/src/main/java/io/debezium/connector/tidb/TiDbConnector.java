/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;

/**
 * A Kafka Connect source connector that captures changes from a TiDB cluster.
 * <p>
 * TiDB does not expose a MySQL binlog; row changes leave the cluster through TiCDC. This
 * connector therefore consumes the Debezium-format Kafka output of a TiCDC changefeed
 * ({@code protocol=debezium}) and layers the Debezium connector framework on top of it — see
 * {@link TiDbConnectorConfig} for the rationale and DBZ-6269 for the design discussion.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbConnector.class);

    private Configuration config;

    public TiDbConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TiDbConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        final Configuration config = Configuration.from(props);

        if (!config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.config = config;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration available", maxTasks);
            return Collections.emptyList();
        }
        // The connector consumes a single TiCDC changefeed and so supports a single task only
        return List.of(config.asMap());
    }

    @Override
    public void stop() {
        this.config = null;
    }

    @Override
    public ConfigDef config() {
        return TiDbConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(TiDbConnectorConfig.ALL_FIELDS);
    }
}
