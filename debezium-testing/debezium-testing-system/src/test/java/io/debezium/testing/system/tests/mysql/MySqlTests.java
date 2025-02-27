/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mysql;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_USERNAME;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tests.binlog.BinlogDBTests;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.mysql.MySqlController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

public abstract class MySqlTests extends BinlogDBTests {

    private final MySqlController dbController;

    public MySqlTests(
                      KafkaController kafkaController,
                      KafkaConnectController connectController,
                      ConnectorConfigBuilder connectorConfig,
                      KafkaAssertions<?, ?> assertions,
                      MySqlController dbController) {
        super(kafkaController, connectController, connectorConfig, assertions);
        this.dbController = dbController;
    }

    public String getDbUserName() {
        return DATABASE_MYSQL_USERNAME;
    }

    public String getDbPassword() {
        return DATABASE_MYSQL_PASSWORD;
    }

    public SqlDatabaseController getDbController() {
        return dbController;
    }

    public void waitForSnapshot() {
        connectController.getMetricsReader().waitForMySqlSnapshot(connectorConfig.getDbServerName());
    }
}
