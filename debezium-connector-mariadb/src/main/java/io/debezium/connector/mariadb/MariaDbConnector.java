/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnector;
import io.debezium.connector.mariadb.jdbc.MariaDbConnection;
import io.debezium.connector.mariadb.jdbc.MariaDbConnectionConfiguration;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;

/**
 * A Debezium source connector that creates tasks and reads changes from MariaDB's binary transaction logs,
 * generating change events.
 *
 * @author Chris Cranford
 */
public class MariaDbConnector extends BinlogConnector<MariaDbConnectorConfig> {

    public MariaDbConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MariaDbConnectorTask.class;
    }

    @Override
    public ConfigDef config() {
        return MariaDbConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MariaDbConnectorConfig.ALL_FIELDS);
    }

    @Override
    protected MariaDbConnection createConnection(Configuration config, MariaDbConnectorConfig connectorConfig) {
        return new MariaDbConnection(new MariaDbConnectionConfiguration(config), new MariaDbFieldReader(connectorConfig));
    }

    @Override
    protected MariaDbConnectorConfig createConnectorConfig(Configuration config) {
        return new MariaDbConnectorConfig(config);
    }
}
